def argparsable(cls_desc):
    def wrapper(cls):
        subcommands = {}

        for real_name, item in cls.__dict__.items():
            name = real_name.replace('_', '-')

            try:
                desc = item._argparse_desc
            except AttributeError:
                continue

            try:
                args = list(reversed(item._argparse_args))
            except AttributeError:
                args = []

            subcommands[name] = desc, args

        cls._argparse_desc = cls_desc
        cls._argparse_subcommands = subcommands

        return cls

    return wrapper

def argument(*args, **kwargs):
    def wrapper(f):
        try:
            f._argparse_args.append((args, kwargs))
        except AttributeError:
            f._argparse_args = [(args, kwargs)]

        return f

    return wrapper

def description(desc):
    def wrapper(f):
        f._argparse_desc = desc

        return f

    return wrapper
