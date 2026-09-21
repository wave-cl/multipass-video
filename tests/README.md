# Browser reproductions

`repro-autoplay-blocked.html` loads the real viewer with `play()` refused the
way a browser that blocks autoplay refuses it (Brave by default, Chrome
before a click). Copy it into the media dir of a running server and open
`/media/repro-autoplay-blocked.html` during an intermission: the page must
end at "Tap for sound" after two attempts, not spin. Before v0.3.11 it
called `play()` without limit and the tab died.
