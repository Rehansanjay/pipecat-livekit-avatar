"""LiveKitAvatarAudioOutput in a real Pipecat pipeline, against a fake LiveKit
room that records what would go over the wire and lets the test play the
avatar's side of the protocol (lk.playback_started / lk.playback_finished)."""

import asyncio
import json
from dataclasses import dataclass, field

import pytest

from pipecat.frames.frames import (
    BotStartedSpeakingFrame,
    BotStoppedSpeakingFrame,
    EndFrame,
    InterruptionFrame,
    OutputAudioRawFrame,
    TTSAudioRawFrame,
    TTSStartedFrame,
    TTSStoppedFrame,
)
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

from pipecat_livekit_avatar import (
    AUDIO_STREAM_TOPIC,
    RPC_CLEAR_BUFFER,
    RPC_PLAYBACK_FINISHED,
    RPC_PLAYBACK_STARTED,
    LiveKitAvatarAudioOutput,
)

VISITOR = "visitor-1"
CHUNK = b"\x01\x00" * 480  # 20 ms at 24 kHz


@dataclass
class FakeWriter:
    name: str
    topic: str
    destinations: list
    attributes: dict
    data: bytearray = field(default_factory=bytearray)
    closed: bool = False

    async def write(self, b: bytes):
        assert not self.closed
        self.data.extend(b)

    async def aclose(self):
        self.closed = True


@dataclass
class Rpc:
    caller_identity: str
    payload: str
    method: str
    request_id: str = "r"
    response_timeout: float = 5.0


class FakeLocal:
    def __init__(self):
        self.streams: list[FakeWriter] = []
        self.rpcs: list[tuple[str, str]] = []
        self.handlers = {}
        self.on_rpc = None  # the avatar's reaction to an RPC from the bot

    async def stream_bytes(self, name, *, topic="", destination_identities=None, attributes=None, **_):
        w = FakeWriter(name, topic, list(destination_identities or []), dict(attributes or {}))
        self.streams.append(w)
        return w

    async def perform_rpc(self, *, destination_identity, method, payload, **_):
        self.rpcs.append((destination_identity, method))
        if self.on_rpc:
            await self.on_rpc(method)
        return "ok"

    def register_rpc_method(self, method, handler):
        self.handlers[method] = handler

    async def page_calls(self, method, payload="", caller=VISITOR):
        """The avatar page invoking one of the bot's RPC methods."""
        return await self.handlers[method](Rpc(caller, payload, method))


class FakeRoom:
    def __init__(self):
        self.local_participant = FakeLocal()
        self.remote_participants = {VISITOR: object()}


class Sink(FrameProcessor):
    def __init__(self):
        super().__init__()
        self.frames = []

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        self.frames.append(frame)
        await self.push_frame(frame, direction)

    def of(self, kind):
        return [f for f in self.frames if isinstance(f, kind)]


class Upstream(FrameProcessor):
    """Sits before the avatar output and records what comes back upstream."""

    def __init__(self):
        super().__init__()
        self.frames = []

    async def process_frame(self, frame, direction):
        await super().process_frame(frame, direction)
        if direction == FrameDirection.UPSTREAM:
            self.frames.append(frame)
        await self.push_frame(frame, direction)


def audio(n=3, kind=TTSAudioRawFrame):
    return [kind(audio=CHUNK, sample_rate=24000, num_channels=1) for _ in range(n)]


async def run(room, steps, **kwargs):
    out = LiveKitAvatarAudioOutput(room, **kwargs)
    up, sink = Upstream(), Sink()
    task = PipelineTask(Pipeline([up, out, sink]), params=PipelineParams(audio_out_sample_rate=24000))

    async def drive():
        await asyncio.sleep(0.05)
        for s in steps:
            if isinstance(s, (int, float)):
                await asyncio.sleep(s)
            elif callable(s):
                await s()
            elif isinstance(s, list):
                for f in s:
                    await task.queue_frame(f)
            else:
                await task.queue_frame(s)
        await asyncio.sleep(0.1)
        await task.queue_frame(EndFrame())

    await asyncio.wait_for(asyncio.gather(PipelineRunner(handle_sigint=False).run(task), drive()), 20)
    return out, up, sink


async def test_voice_goes_to_the_avatar_stream_not_the_transport():
    room = FakeRoom()
    _, _, sink = await run(room, [TTSStartedFrame(), audio(3), TTSStoppedFrame(), 0.1])

    [s] = room.local_participant.streams
    assert s.topic == AUDIO_STREAM_TOPIC
    assert s.destinations == [VISITOR]
    assert s.attributes == {"sample_rate": "24000", "num_channels": "1"}
    assert bytes(s.data) == CHUNK * 3
    assert s.closed, "the segment's stream is closed at TTSStoppedFrame"
    assert not sink.of(OutputAudioRawFrame), "no bot audio reaches the transport"
    assert sink.of(TTSStoppedFrame)


async def test_bot_speaking_follows_the_avatar_playback():
    room = FakeRoom()
    lp = room.local_participant
    _, up, sink = await run(
        room,
        [
            TTSStartedFrame(), audio(2), TTSStoppedFrame(), 0.1,
            lambda: lp.page_calls(RPC_PLAYBACK_STARTED), 0.1,
            lambda: lp.page_calls(RPC_PLAYBACK_FINISHED, json.dumps({"playback_position": 0.04, "interrupted": False})),
            0.1,
        ],
    )
    for frames in (sink.frames, up.frames):
        kinds = [type(f) for f in frames if isinstance(f, (BotStartedSpeakingFrame, BotStoppedSpeakingFrame))]
        assert kinds == [BotStartedSpeakingFrame, BotStoppedSpeakingFrame]


async def test_interruption_clears_the_avatar_and_new_speech_opens_a_new_stream():
    room = FakeRoom()
    lp = room.local_participant

    async def avatar(method):  # the page stops and reports the interrupted segment
        if method == RPC_CLEAR_BUFFER:
            await lp.page_calls(RPC_PLAYBACK_FINISHED, json.dumps({"playback_position": 0.02, "interrupted": True}))

    lp.on_rpc = avatar
    _, _, sink = await run(
        room,
        [
            TTSStartedFrame(), audio(2), 0.05,
            lambda: lp.page_calls(RPC_PLAYBACK_STARTED), 0.05,
            InterruptionFrame(), 0.1,
            TTSStartedFrame(), audio(1), TTSStoppedFrame(), 0.1,
        ],
    )
    assert lp.rpcs == [(VISITOR, RPC_CLEAR_BUFFER)]
    assert len(lp.streams) == 2 and all(s.closed for s in lp.streams)
    assert bytes(lp.streams[0].data) == CHUNK * 2 and bytes(lp.streams[1].data) == CHUNK
    assert sink.of(BotStoppedSpeakingFrame), "the interrupted playback ends the bot's turn"


async def test_missing_playback_finished_after_clear_times_out():
    room = FakeRoom()
    lp = room.local_participant
    _, _, sink = await run(
        room,
        [
            TTSStartedFrame(), audio(2), 0.05,
            lambda: lp.page_calls(RPC_PLAYBACK_STARTED), 0.05,
            InterruptionFrame(), 0.5,
        ],
        clear_buffer_timeout=0.2,
    )
    assert lp.rpcs == [(VISITOR, RPC_CLEAR_BUFFER)]
    assert sink.of(BotStoppedSpeakingFrame), "the bot isn't left 'speaking' forever"


async def test_rpc_from_another_participant_is_rejected():
    room = FakeRoom()
    lp = room.local_participant
    answers = []

    async def stranger():
        answers.append(await lp.page_calls(RPC_PLAYBACK_STARTED, caller="someone-else"))

    _, _, sink = await run(room, [TTSStartedFrame(), audio(1), TTSStoppedFrame(), 0.05, stranger, 0.1])
    assert answers == ["reject"]
    assert not sink.of(BotStartedSpeakingFrame)


async def test_without_playback_started_rpc_speaking_starts_with_the_audio():
    room = FakeRoom()
    _, _, sink = await run(room, [TTSStartedFrame(), audio(1), TTSStoppedFrame(), 0.1], wait_playback_start=False)
    assert sink.of(BotStartedSpeakingFrame)


async def test_no_interruption_rpc_when_nothing_is_playing():
    room = FakeRoom()
    await run(room, [InterruptionFrame(), 0.1])
    assert room.local_participant.rpcs == []
