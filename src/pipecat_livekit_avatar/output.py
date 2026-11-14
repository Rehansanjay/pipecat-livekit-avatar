"""Send a Pipecat bot's voice to a LiveKit avatar over data streams.

LiveKit avatars that render in the visitor's browser (MeshLite) or in an avatar
worker (the avatar plugins built on LiveKit Agents' ``DataStreamAudioOutput``)
take the agent's voice over LiveKit's avatar data-stream protocol instead of an
audio track:

* bot -> avatar: one ``lk.audio_stream`` byte stream per speech segment, 16-bit
  PCM, with ``sample_rate`` and ``num_channels`` stream attributes;
* bot -> avatar: RPC ``lk.clear_buffer`` when the user interrupts;
* avatar -> bot: RPC ``lk.playback_started`` and ``lk.playback_finished``
  (``{"playback_position": float, "interrupted": bool}``).

``LiveKitAvatarAudioOutput`` speaks that protocol for a Pipecat pipeline running
on ``LiveKitTransport``. Put it after the TTS and before ``transport.output()``:
it takes the bot's audio out of the pipeline (so the room carries no bot audio
track; the avatar plays the voice) and reports ``BotStartedSpeakingFrame`` /
``BotStoppedSpeakingFrame`` from the avatar's playback, which is what the user
actually hears.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Callable, Optional

from loguru import logger

from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    CancelFrame,
    EndFrame,
    Frame,
    InterruptionFrame,
    OutputAudioRawFrame,
    TTSStoppedFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

RPC_CLEAR_BUFFER = "lk.clear_buffer"
RPC_PLAYBACK_FINISHED = "lk.playback_finished"
RPC_PLAYBACK_STARTED = "lk.playback_started"
AUDIO_STREAM_TOPIC = "lk.audio_stream"


class LiveKitAvatarAudioOutput(FrameProcessor):
    """Stream the bot's audio to a LiveKit avatar participant's data stream.

    Args:
        room: The ``livekit.rtc.Room`` the bot is in, or a callable returning it
            (the room only exists once the transport has connected). For
            ``LiveKitTransport`` use :func:`room_of`.
        destination_identity: Identity of the participant that renders the
            avatar. For a browser avatar (MeshLite) that is the visitor. If
            None, the first remote participant to join is used.
        wait_playback_start: If True, ``BotStartedSpeakingFrame`` waits for the
            avatar's ``lk.playback_started`` RPC. If False it is sent when the
            first audio of a segment goes out (for avatars that only report
            ``lk.playback_finished``).
        clear_buffer_timeout: After ``lk.clear_buffer``, how long to wait for
            the avatar's ``lk.playback_finished`` before treating the segment
            as finished anyway.
    """

    def __init__(
        self,
        room: Any | Callable[[], Any],
        *,
        destination_identity: Optional[str] = None,
        wait_playback_start: bool = True,
        clear_buffer_timeout: float = 2.0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._room_or_provider = room
        self._destination = destination_identity
        self._wait_playback_start = wait_playback_start
        self._clear_buffer_timeout = clear_buffer_timeout

        self._writer = None  # ByteStreamWriter for the segment being sent
        self._segments_pending = 0  # segments sent but not reported finished
        self._bot_speaking = False
        self._rpc_registered = False
        self._clear_task: Optional[asyncio.Task] = None
        self._playback_done = asyncio.Event()
        self._playback_done.set()

    # ------------------------------------------------------------------ room

    @property
    def room(self):
        r = self._room_or_provider
        return r() if callable(r) else r

    def _local(self):
        return self.room.local_participant

    def _resolve_destination(self) -> Optional[str]:
        if self._destination:
            return self._destination
        participants = getattr(self.room, "remote_participants", {}) or {}
        if participants:
            self._destination = next(iter(participants))
            logger.debug(f"{self}: sending avatar audio to {self._destination}")
        return self._destination

    def _register_rpc(self) -> None:
        if self._rpc_registered:
            return
        local = self._local()
        local.register_rpc_method(RPC_PLAYBACK_FINISHED, self._on_playback_finished)
        local.register_rpc_method(RPC_PLAYBACK_STARTED, self._on_playback_started)
        self._rpc_registered = True

    # ---------------------------------------------------------- rpc from page

    def _from_destination(self, data) -> bool:
        if self._destination and data.caller_identity != self._destination:
            logger.warning(f"{self}: {data.method} from unexpected participant {data.caller_identity}")
            return False
        return True

    async def _on_playback_started(self, data) -> str:
        if not self._from_destination(data):
            return "reject"
        await self._set_bot_speaking(True)
        return "ok"

    async def _on_playback_finished(self, data) -> str:
        if not self._from_destination(data):
            return "reject"
        try:
            event = json.loads(data.payload or "{}")
        except json.JSONDecodeError:
            event = {}
        logger.debug(
            f"{self}: playback finished at {event.get('playback_position')} s"
            f"{' (interrupted)' if event.get('interrupted') else ''}"
        )
        self._segment_finished()
        return "ok"

    def _segment_finished(self) -> None:
        self._segments_pending = max(0, self._segments_pending - 1)
        if self._segments_pending == 0:
            self._playback_done.set()
            self.create_task(self._set_bot_speaking(False), name="avatar_bot_stopped")

    async def _set_bot_speaking(self, speaking: bool) -> None:
        if speaking == self._bot_speaking:
            return
        self._bot_speaking = speaking
        await self.broadcast_frame(BotStartedSpeakingFrame if speaking else BotStoppedSpeakingFrame)

    # ----------------------------------------------------------- audio out

    async def _write_audio(self, frame: OutputAudioRawFrame) -> None:
        if self._clear_task and not self._clear_task.done():
            # Finish the interrupted playback before newer audio goes out.
            await asyncio.shield(self._clear_task)

        destination = self._resolve_destination()
        if not destination:
            logger.warning(f"{self}: no avatar participant in the room yet, dropping audio")
            return
        self._register_rpc()

        if self._writer is None:
            self._writer = await self._local().stream_bytes(
                name=f"AUDIO_{uuid.uuid4().hex[:12]}",
                topic=AUDIO_STREAM_TOPIC,
                destination_identities=[destination],
                attributes={
                    "sample_rate": str(frame.sample_rate),
                    "num_channels": str(frame.num_channels),
                },
            )
            self._segments_pending += 1
            self._playback_done.clear()
            if not self._wait_playback_start:
                await self._set_bot_speaking(True)
        await self._writer.write(bytes(frame.audio))

    async def _end_segment(self) -> None:
        if self._writer is None:
            return
        writer, self._writer = self._writer, None
        try:
            await writer.aclose()
        except Exception as e:
            logger.warning(f"{self}: closing audio stream failed: {e}")

    async def _clear_buffer(self) -> None:
        await self._end_segment()
        if not self._segments_pending or not self._destination:
            return
        if self._clear_task and not self._clear_task.done():
            return
        self._clear_task = self.create_task(self._clear_buffer_task(), name="avatar_clear_buffer")

    async def _clear_buffer_task(self) -> None:
        timeout = self._clear_buffer_timeout
        try:
            await self._local().perform_rpc(
                destination_identity=self._destination, method=RPC_CLEAR_BUFFER, payload=""
            )
        except Exception as e:
            logger.error(f"{self}: {RPC_CLEAR_BUFFER} failed: {e}")
            timeout = 0
        try:
            await asyncio.wait_for(self._playback_done.wait(), timeout)
        except asyncio.TimeoutError:
            logger.warning(f"{self}: no {RPC_PLAYBACK_FINISHED} after {RPC_CLEAR_BUFFER}, treating playback as done")
            self._segments_pending = 1
            self._segment_finished()

    # ------------------------------------------------------------- frames

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        await super().process_frame(frame, direction)

        if direction == FrameDirection.DOWNSTREAM and isinstance(frame, OutputAudioRawFrame):
            # The avatar plays the voice: the bot's audio doesn't continue to the transport.
            await self._write_audio(frame)
            return

        if isinstance(frame, InterruptionFrame):
            await self._clear_buffer()
        elif isinstance(frame, TTSStoppedFrame):
            await self._end_segment()
        elif isinstance(frame, (EndFrame, CancelFrame)):
            await self._end_segment()

        await self.push_frame(frame, direction)


def room_of(transport) -> Callable[[], Any]:
    """The room of a ``LiveKitTransport``, for ``LiveKitAvatarAudioOutput(room=...)``."""
    return lambda: transport._client.room
