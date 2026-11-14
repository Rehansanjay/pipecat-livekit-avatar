# pipecat-livekit-avatar

Use LiveKit avatars from a **Pipecat** bot.

Several avatars don't take the bot's voice as an audio track. They take it over
LiveKit's avatar data-stream protocol, the one LiveKit Agents uses in
`DataStreamAudioOutput`:

- bot → avatar: one `lk.audio_stream` byte stream per speech segment (16-bit PCM,
  `sample_rate` and `num_channels` as stream attributes)
- bot → avatar: RPC `lk.clear_buffer` when the user interrupts
- avatar → bot: RPC `lk.playback_started` and `lk.playback_finished`
  (`{"playback_position": float, "interrupted": bool}`)

That includes avatars rendered in the visitor's browser, such as
[MeshLite](https://meshlite.io). Until now they worked with LiveKit Agents
only. `LiveKitAvatarAudioOutput` speaks the same protocol for a Pipecat pipeline
running on `LiveKitTransport`, so the same avatar page works unchanged.

## Install

```bash
pip install pipecat-livekit-avatar
```

Requires `pipecat-ai[livekit]` 1.x (tested with 1.0.0 and 1.16.0).

## Use

Put it after the TTS and before `transport.output()`:

```python
from pipecat.transports.livekit.transport import LiveKitParams, LiveKitTransport
from pipecat_livekit_avatar import LiveKitAvatarAudioOutput, room_of

transport = LiveKitTransport(url, token, room_name, params=LiveKitParams(audio_in_enabled=True))

avatar = LiveKitAvatarAudioOutput(
    room_of(transport),
    destination_identity="visitor-1",  # the participant that renders the avatar; None = first to join
)

pipeline = Pipeline([
    transport.input(),
    stt,
    user_aggregator,
    llm,
    tts,
    avatar,              # the voice goes to the avatar, not to an audio track
    transport.output(),
    assistant_aggregator,
])
```

What it does:

- Each spoken segment goes to the avatar as one `lk.audio_stream`, closed at
  `TTSStoppedFrame`. The bot's audio does not continue to `transport.output()`, so
  the room carries no bot audio track: the avatar plays the voice.
- On `InterruptionFrame` it calls `lk.clear_buffer` on the avatar and waits for its
  `lk.playback_finished` (up to `clear_buffer_timeout`, 2 s) before newer audio goes
  out.
- `BotStartedSpeakingFrame` and `BotStoppedSpeakingFrame` come from the avatar's
  `lk.playback_started` / `lk.playback_finished`, so the pipeline's idea of "the bot is
  speaking" matches what the user actually hears. Set `wait_playback_start=False` for
  avatars that only report `lk.playback_finished`.
- RPCs from any participant other than the avatar's are rejected.

## Browser avatars (MeshLite)

The visitor page is the same as for LiveKit Agents: mount the avatar, then
`avatar.attachLiveKit(room)` before `room.connect(...)`. See MeshLite's
[browser-side guide](https://github.com/meshlite-avatar/livekit-plugins-meshlite#browser-side).
Give the bot's `LiveKitAvatarAudioOutput` the visitor's identity.

## Tests

```bash
pip install -e ".[test]"
pytest
```

The tests run a real Pipecat pipeline offline against a fake room that records
what would go over the wire and plays the avatar's side of the protocol:
streams and their attributes, the clear-buffer RPC on interruption, bot speaking
state from playback RPCs, the timeout when an avatar never answers, and rejecting
RPCs from other participants. They pass on Pipecat 1.0.0 and 1.16.0.

Not yet tested against a live LiveKit room with a real avatar.
