class Colorizer:
    ANSI_ESCAPE_CODES = {
        'reset': 0,
        'bold': 1,
        # Foreground regular.
        'fg_black': 30,
        'fg_red': 31,
        'fg_green': 32,
        'fg_yellow': 33,
        'fg_blue': 34,
        'fg_magenta': 35,
        'fg_cyan': 36,
        'fg_white': 37,
        # Background regular.
        'bg_black': 40,
        'bg_red': 41,
        'bg_green': 42,
        'bg_yellow': 43,
        'bg_blue': 44,
        'bg_magenta': 45,
        'bg_cyan': 46,
        'bg_white': 47,
        # Foreground bright.
        'fg_b_black': 90,
        'fg_b_red': 91,
        'fg_b_green': 92,
        'fg_b_yellow': 93,
        'fg_b_blue': 94,
        'fg_b_magenta': 95,
        'fg_b_cyan': 96,
        'fg_b_white': 97,
        # Background bright.
        'bg_b_black': 100,
        'bg_b_red': 101,
        'bg_b_green': 102,
        'bg_b_yellow': 103,
        'bg_b_blue': 104,
        'bg_b_magenta': 105,
        'bg_b_cyan': 106,
        'bg_b_white': 107,
    }

    THEMES = {
        'error': ['fg_b_white', 'bg_red'],
        'good': ['fg_b_green'],
        'notice': ['fg_cyan'],
        'warning': ['fg_yellow'],
    }

    def __init__(self, use_colors):
        self.use_colors = use_colors

    def _expand_name(self, name):
        if name not in self.THEMES:
            yield self.ANSI_ESCAPE_CODES[name]

            return

        for subname in self.THEMES[name]:
            yield from self._expand_name(subname)

    def __call__(self, *names, p=None):
        if self.use_colors:
            codes = []

            for name in names:
                codes.extend(map(str, self._expand_name(name)))

            string = '\x1b[{}m'.format(';'.join(codes))
        else:
            string = ''

        if p is not None:
            print(string, end='')
        else:
            return string
