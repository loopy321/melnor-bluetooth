# See _getBattValue from DataUtils.java
def parse_battery_value(data: bytes) -> int:
    """Converts the little endian 2-byte array to the battery life %.
    Raises ValueError if the input is missing or invalid.
    """
    # Validate input
    if not data or len(data) < 2:
        raise ValueError("Battery payload missing or invalid")

    b0 = data[0] & 0xFF
    b1 = data[1] & 0xFF

    # Check for sentinel bytes (0xEE 0xEE means 'no reading yet')
    if b0 == 0xEE and b1 == 0xEE:
        return 0

    # Original Melnor conversion formula
    rawVal = ((b0 + b1 / 256) - 2.35) * 181.81818181818187

    if rawVal > 100:
        return 100
    elif rawVal < 0:
        return 0

    return int(rawVal)
