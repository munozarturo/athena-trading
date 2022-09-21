from turtle import back
from colorama import init, Fore, Back

init(autoreset=True)

_hash = hash((1, 2, 3))

def colorize_hash(_hash: int) -> str:
    CSI = '\033['

    def code_to_colour(code: int) -> str:
        return CSI + str(code) + "m"

    def colour_byte(_byte: int) -> str:
        fore = _byte % 8
        back = max(0, _byte - 8)

        back = fore
                     
        fore += 30
        back += 40
        
        return f"{code_to_colour(fore)}{code_to_colour(back)}{hex(_byte)[2:]}"

    _colorized = ""
    for byte in _hash.to_bytes(8, 'big', signed=True):
        first = byte >> 4
        second = byte & 0x0f
        
        _colorized += f"{colour_byte(first)}{colour_byte(second)}"
        
    return _colorized