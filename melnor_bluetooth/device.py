""" Device interactions for Melnor bluetooth devices. """

from __future__ import annotations

import asyncio
import logging
import struct
from datetime import datetime, time
from typing import List, Optional, Union

from bleak.backends.device import BLEDevice
from bleak.exc import BleakError
from bleak_retry_connector import BleakClient  # type: ignore - this is a valid import
from bleak_retry_connector import establish_connection
from deprecated import deprecated

from .constants import (
    BATTERY_UUID,
    MANUFACTURER_UUID,
    UPDATED_AT_UUID,
    VALVE_0_MODE_UUID,
    VALVE_1_MODE_UUID,
    VALVE_2_MODE_UUID,
    VALVE_3_MODE_UUID,
    VALVE_MANUAL_SETTINGS_UUID,
    VALVE_MANUAL_STATES_UUID,
    VALVE_ON_OFF_UUID,
)
from .models.frequency import Frequency
from .utils import date
from .utils.battery import parse_battery_value
from .utils.lock import GLOBAL_BLUETOOTH_LOCK, bluetooth_lock

_LOGGER = logging.getLogger(__name__)


class Valve:
    """Wrapper class to handle interacting with individual valves on a Melnor timer"""

    _device: "Device"
    _frequency: Frequency
    _id: int
    _is_watering: bool
    _is_frequency_schedule_enabled: bool
    _manual_minutes: int

    def __init__(self, identifier: int, device) -> None:
        self._device = device
        self._frequency = Frequency()
        self._id = identifier
        self._is_watering = False
        self._is_frequency_schedule_enabled = False
        self._manual_minutes = 20
        self._end_time = 0

    def update_state(self, raw_bytes: Optional[bytes], uuid: str) -> None:
        """Update the state of the valve from the raw bytes (defensive against empty/short reads)."""

        if not raw_bytes:
            _LOGGER.debug("Valve %s: empty payload for UUID %s; skipping update", self._id, uuid)
            return

        # helper that validates length before unpack
        def _unpack(fmt: str, data: bytes, offset: int = 0):
            needed = struct.calcsize(fmt)
            if offset < 0 or (offset + needed) > len(data):
                _LOGGER.debug(
                    "Valve %s: insufficient data for %s at offset %s (len=%s) UUID=%s",
                    self._id, fmt, offset, len(data), uuid
                )
                raise ValueError("insufficient data")
            return struct.unpack_from(fmt, data, offset)

        offset = self._id * 5

        if uuid == VALVE_MANUAL_SETTINGS_UUID:
            # payload slice is 5 bytes per valve:
            # 0: bool is_watering
            # 1-2: uint16 manual_watering_time
            # 3-4: duplicate of 1-2
            try:
                self._is_watering = _unpack(">?", raw_bytes, offset)[0]
                self._manual_minutes = _unpack(">H", raw_bytes, offset + 1)[0]
            except ValueError:
                return

        elif uuid == VALVE_MANUAL_STATES_UUID:
            # payload slice (per valve):
            # 0   - unknown (0-2)
            # 1-4 - uint32 timestamp end time (epoch seconds)
            try:
                parsed_time = _unpack(">I", raw_bytes, offset + 1)[0]
            except ValueError:
                return
            self._end_time = parsed_time - date.time_shift() if parsed_time != 0 else 0

        elif uuid == VALVE_ON_OFF_UUID:
            # per valve 1 byte boolean at index == valve_id
            try:
                self._is_frequency_schedule_enabled = _unpack(">?", raw_bytes, self._id)[0]
            except ValueError:
                return

        elif (
            (self._id == 0 and uuid == VALVE_0_MODE_UUID)
            or (self._id == 1 and uuid == VALVE_1_MODE_UUID)
            or (self._id == 2 and uuid == VALVE_2_MODE_UUID)
            or (self._id == 3 and uuid == VALVE_3_MODE_UUID)
        ):
            # Let Frequency handle its own validation
            self._frequency.update_state(raw_bytes)

    @property
    def frequency_bytes(self) -> Optional[bytes]:
        """Returns the frequency bytes"""
        if self._frequency is not None:
            return self._frequency.to_bytes()
        return None

    @property
    def id(self) -> int:
        """Returns the raw valve id"""
        return self._id

    @property
    def is_watering(self) -> bool:
        """Returns the zone watering state"""
        return self._is_watering == 1

    @property
    def schedule_enabled(self) -> bool:
        """Returns the zone watering state"""
        return self._is_frequency_schedule_enabled

    @property
    def frequency(self) -> Frequency:
        """Returns the zone watering state"""
        return self._frequency

    @property
    def next_cycle(self) -> Optional[datetime]:
        """Returns the next cycle time"""
        if self.schedule_enabled:
            return self._frequency.next_run_time
        return None

    @is_watering.setter
    @deprecated(version="0.0.18", reason="Use set_is_watering instead")
    def is_watering(self, value: bool) -> None:
        """@deprecated"""
        self._is_watering = value

    @bluetooth_lock
    async def set_is_watering(self, value: bool) -> None:
        """Atomically sets zone watering state"""
        self._is_watering = value
        await self._device._unsafe_push_state()  # pylint: disable=protected-access

    @property
    def manual_watering_minutes(self) -> int:
        """Returns the number of minutes the zone has been manually watering"""
        return self._manual_minutes

    @manual_watering_minutes.setter
    @deprecated(version="0.0.18", reason="Use set_manual_watering_minutes instead")
    def manual_watering_minutes(self, value: int) -> None:
        """@deprecated"""
        self._manual_minutes = value

    @bluetooth_lock
    async def set_manual_watering_minutes(self, value: int) -> None:
        """Atomically set the number of minutes the valve should water."""
        self._manual_minutes = value
        await self._device._unsafe_push_state()  # pylint: disable=protected-access

    @bluetooth_lock
    async def set_frequency_interval_hours(self, value: int) -> None:
        """Atomically set the frequency interval hours"""
        if self._frequency is not None:
            self._frequency.interval_hours = value
            await self._device._unsafe_push_state()  # pylint: disable=protected-access

    @bluetooth_lock
    async def set_frequency_duration_minutes(self, value: int) -> None:
        """Atomically set the frequency duration"""
        if self._frequency is not None:
            self._frequency.duration_minutes = value
            await self._device._unsafe_push_state()  # pylint: disable=protected-access

    @bluetooth_lock
    async def set_frequency_start_time(self, value: time) -> None:
        """Atomically set the frequency start time"""
        if self._frequency is not None:
            self._frequency.start_time = value
            await self._device._unsafe_push_state()  # pylint: disable=protected-access

    @bluetooth_lock
    async def set_frequency_enabled(self, value: bool) -> None:
        """Atomically set the frequency enabled state"""
        self._is_frequency_schedule_enabled = value
        await self._device._unsafe_push_state()  # pylint: disable=protected-access

    @property
    def watering_end_time(self) -> int:
        """Read-only unix timestamp in seconds when watering will end."""
        return self._end_time

    def _manual_setting_bytes(self) -> bytes:
        """Returns the 5 byte payload to be written to the device"""
        return struct.pack(
            ">?HH",
            self._is_watering,
            self._manual_minutes,
            self._manual_minutes,
        )

    def __str__(self) -> str:
        return (
            f"      Valve(id={self._id}|"
            + f"is_watering={self._is_watering}|"
            + f"manual_minutes={self._manual_minutes}|"
            + f"seconds_left={self._end_time}"
            + ")"
        )


class Device:
    """A wrapper class to interact with Melnor Bluetooth devices"""

    _battery: int
    _ble_device: BLEDevice
    _brand: str
    _connection: BleakClient
    _connection_lock = asyncio.Lock()
    _is_connected: bool
    _model: str
    _sensor: bool
    _valves: List[Valve]
    _valve_count: int

    def __init__(self, ble_device: BLEDevice) -> None:
        self._battery = 0
        self._ble_device = ble_device
        self._is_connected = False
        self._mac = ble_device.address
        self._valves = []

        # The 1 and 2 valve devices still use 4 valve bytes, so we mirror that
        for i in range(4):
            self._valves.append(Valve(i, self))

    async def _read_model(self):
        """Initializes the device"""
        if not self._is_connected:
            return

        try:
            manufacturer_data = await self._connection.read_gatt_char(MANUFACTURER_UUID)
        except BleakError:
            _LOGGER.error("Failed to read model from %s", self._mac)
            return

        string = manufacturer_data.decode("utf-8")

        self._model = string[0:5]
        self._valve_count = int(string[6:7])

    def disconnected_callback(self, client):  # pylint: disable=unused-argument
        """Callback for when the device is disconnected"""
        _LOGGER.warning("Disconnected from %s", self._mac)
        self._is_connected = False

    @bluetooth_lock
    async def connect(self, retry_attempts=4) -> None:
        """Connects to the device"""

        if self._is_connected or self._connection_lock.locked():
            return

        async with self._connection_lock:
            try:
                _LOGGER.debug("Connecting to %s", self._mac)

                self._connection = await establish_connection(
                    client_class=BleakClient,
                    device=self._ble_device,
                    name=self._mac,
                    disconnected_callback=self.disconnected_callback,
                    max_attempts=retry_attempts,
                    use_services_cache=True,
                )

                self._is_connected = True

                # Work around BlueZ advertisement quirks
                await self._read_model()

                _LOGGER.debug("Successfully connected to %s", self._mac)

            except BleakError:
                _LOGGER.error("Failed to connect to %s", self._mac)
                self._is_connected = False

    @bluetooth_lock
    async def disconnect(self) -> None:
        """Disconnects the device"""
        await self._connection.disconnect()

    async def fetch_state(self) -> None:
        """Updates the state of the device with the given bytes"""

        if not self._is_connected:
            await self.connect(retry_attempts=1)

        async with GLOBAL_BLUETOOTH_LOCK:
            uuids = [
                BATTERY_UUID,
                VALVE_MANUAL_SETTINGS_UUID,
                VALVE_MANUAL_STATES_UUID,
                VALVE_ON_OFF_UUID,
                VALVE_0_MODE_UUID,
                VALVE_1_MODE_UUID,
                VALVE_2_MODE_UUID,
                VALVE_3_MODE_UUID,
            ]

            try:
                # allow exceptions in results so one bad read doesn't kill the whole pass
                results: List[Union[bytes, Exception, None]] = await asyncio.gather(
                    *[self._read(uuid) for uuid in uuids],
                    return_exceptions=True,
                )

                for i, payload in enumerate(results):
                    uuid = uuids[i]

                    # Handle exceptions and empty reads gracefully
                    if isinstance(payload, Exception):
                        _LOGGER.debug("Read exception for %s: %r", uuid, payload)
                        continue
                    if not payload:
                        _LOGGER.debug("No data for UUID %s; skipping", uuid)
                        continue

                    # Coerce to bytes if backend returns bytearray/memoryview
                    if not isinstance(payload, (bytes, bytearray, memoryview)):
                        try:
                            payload = bytes(payload)  # type: ignore[arg-type]
                        except Exception:
                            _LOGGER.debug("Failed to coerce payload type for %s; skipping", uuid)
                            continue
                    if isinstance(payload, (bytearray, memoryview)):
                        payload = bytes(payload)

                    if uuid == BATTERY_UUID:
                        try:
                            self._battery = parse_battery_value(payload)
                        except Exception as err:
                            _LOGGER.debug("Battery parse failed for %s: %r", self._mac, err)
                        continue

                    # Update all valves with this UUID's payload
                    for valve in self._valves:
                        try:
                            valve.update_state(payload, uuid)
                        except Exception as err:
                            _LOGGER.debug(
                                "Valve %s update skipped for UUID %s due to error: %r",
                                valve.id, uuid, err
                            )

            except BleakError as error:
                # Only re-raise if the device is still connected
                if self._is_connected:
                    raise error

    async def _read(self, uuid: str) -> Optional[bytes]:
        """Reads the given characteristic from the device"""
        if not self._is_connected:
            return None

        try:
            return await self._connection.read_gatt_char(uuid)
        except BleakError:
            _LOGGER.error("Failed to read %s from %s", uuid, self._mac)
            return None

    async def _unsafe_push_state(self) -> None:
        """Pushes the new state of the device to the device. WARNING: no internal lock."""

        if not self._is_connected:
            return

        on_off = self._connection.services.get_characteristic(VALVE_MANUAL_SETTINGS_UUID)

        if on_off is not None:
            await self._connection.write_gatt_char(
                on_off.handle,
                (
                    # pylint: disable=protected-access
                    self._valves[0]._manual_setting_bytes()
                    + self._valves[1]._manual_setting_bytes()
                    + self._valves[2]._manual_setting_bytes()
                    + self._valves[3]._manual_setting_bytes()
                ),
                True,
            )

        await self._connection.write_gatt_char(
            VALVE_ON_OFF_UUID,
            struct.pack(
                ">????",
                # pylint: disable=protected-access
                self._valves[0].schedule_enabled,
                self._valves[1].schedule_enabled,
                self._valves[2].schedule_enabled,
                self._valves[3].schedule_enabled,
            ),
            True,
        )

        zone_1_frequency_bytes = self._valves[0].frequency_bytes
        if zone_1_frequency_bytes is not None:
            await self._connection.write_gatt_char(VALVE_0_MODE_UUID, zone_1_frequency_bytes, True)

        zone_2_frequency_bytes = self._valves[1].frequency_bytes
        if zone_2_frequency_bytes is not None:
            await self._connection.write_gatt_char(VALVE_1_MODE_UUID, zone_2_frequency_bytes, True)

        zone_3_frequency_bytes = self._valves[2].frequency_bytes
        if zone_3_frequency_bytes is not None:
            await self._connection.write_gatt_char(VALVE_2_MODE_UUID, zone_3_frequency_bytes, True)

        zone_4_frequency_bytes = self._valves[3].frequency_bytes
        if zone_4_frequency_bytes is not None:
            await self._connection.write_gatt_char(VALVE_3_MODE_UUID, zone_4_frequency_bytes, True)

        updated_at = self._connection.services.get_characteristic(UPDATED_AT_UUID)

        if updated_at is not None:
            await self._connection.write_gatt_char(
                updated_at.handle,
                struct.pack(">I", date.get_timestamp()),
                True,
            )

    async def push_state(self) -> None:
        """Pushes the new state of the device to the device"""
        async with GLOBAL_BLUETOOTH_LOCK:
            await self._unsafe_push_state()

    @property
    def battery_level(self) -> int:
        """Returns the battery level of the device"""
        return self._battery

    @property
    def brand(self) -> str:
        """Returns the manufacturer of the device"""
        return self._brand

    @property
    def is_connected(self) -> bool:
        """Returns whether the device is currently connected"""
        return self._is_connected

    @property
    def mac(self) -> str:
        """Returns the MAC address of the device"""
        return self._mac

    @property
    def model(self) -> str:
        """Returns the name of the device"""
        return self._model

    @property
    def name(self) -> str:
        """Returns the name of the device"""
        return f"{self._valve_count} Valve Timer"

    @property
    def rssi(self) -> int:
        """Returns the RSSI of the device"""
        return self._ble_device.rssi

    @property
    def valve_count(self) -> int:
        """Returns the number of valves on the device"""
        return self._valve_count

    @valve_count.setter
    def valve_count(self, value: int) -> None:
        """Sets the number of valves on the device"""
        self._valve_count = value

    @property
    def zone1(self) -> Valve:
        """Returns the first zone on the device"""
        return self._valves[0]

    @property
    def zone2(self) -> Optional[Valve]:
        """Returns the second zone on the device"""
        if self._valve_count > 1:
            return self._valves[1]
        return None

    @property
    def zone3(self) -> Optional[Valve]:
        """Returns the third zone on the device"""
        if self._valve_count > 2:
            return self._valves[2]
        return None

    @property
    def zone4(self) -> Optional[Valve]:
        """Returns the fourth zone on the device"""
        if self._valve_count > 3:
            return self._valves[3]
        return None

    def update_ble_device(self, ble_device: BLEDevice) -> None:
        """Updates the cached BLEDevice for the device"""
        self._ble_device = ble_device

    def __str__(self) -> str:
        string = f"{self.__class__.__name__}(\n    battery={self._battery}\n    valves=(\n"
        for valve in self._valves:
            string += f"{valve}\n"
        return f"{string}    )\n)"

    def __getitem__(self, key: str) -> Optional[Valve]:
        if key == "zone1":
            return self.zone1
        if key == "zone2":
            return self.zone2
        if key == "zone3":
            return self.zone3
        if key == "zone4":
            return self.zone4
        return None
