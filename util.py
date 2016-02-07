import inspect
import re
import clint.textui
from clint.textui import colored

def caller():
    # note, need extra 'f_back' because this itself is a function...
    frame = inspect.currentframe().f_back.f_back
    return frame.f_globals, frame.f_locals


def fmt(s, context=None):
    """
    Interpolate arbitrary expressions inside '{}' in strings (similar to Ruby or
    Scala). It should go without saying that because this uses `eval`, it is 
    unsafe to use with any user-provided inputs. It is also probably terribly 
    slow because it's using `inspect` to get the caller's local variables.

    Example:
        x = 1
        print fmt('x is {x}, x+1 is {x+1}')
        > x is  1, x+1 is 2
    """
    context = context or caller()
    return re.compile(r"#{(.*?)}").sub(lambda m: str(eval(m.group(1), *context)), str(s))


def puts(s):
    clint.textui.puts(fmt(s, caller()))


def puts_err(s):
    clint.textui.puts_err(fmt(s, caller()))
