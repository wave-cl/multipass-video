# Browser reproductions

`repro-autoplay-blocked.html` loads the real viewer with `play()` refused the
way a browser that blocks autoplay refuses it (Brave by default, Chrome
before a click). Copy it into the media dir of a running server and open
`/media/repro-autoplay-blocked.html` during an intermission: the page must
end at "Tap for sound" after two attempts, not spin. Before v0.3.11 it
called `play()` without limit and the tab died.

`repro-dead-source.html` does the same with the music element's `play()`
throwing `NotSupportedError` ("no supported sources") until the element is
reloaded -- what Samsung's TV browser does to an audio element left idle
while the film plays. Open it, then start an intermission: the music must
play after one reload with no tap prompt.
