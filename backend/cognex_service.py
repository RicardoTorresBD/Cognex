"""Mock Cognex In-Sight camera speaking Native Mode ASCII over TCP.

A real In-Sight job is usually fired with ``SW8`` (Set Event, soft trigger).
Some cells send the literal word ``Trigger``. This simulator accepts both,
waits 500 ms to imitate acquire time, then returns one comma-separated line:

    1,PASS,ON,OFF,ON,ON,OFF

The leading ``1`` is the Native Mode success code. The next field is the
inspection judgment, followed by five DIP switch positions.

``CognexCameraMock`` is the camera. ``CognexCameraClient`` is what the API uses,
so the same client can later point at a real camera by disabling the mock.
"""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass

logger = logging.getLogger(__name__)

_TRIGGER_COMMANDS = {"SW8", "TRIGGER"}
_SWITCH_STATES = ("ON", "OFF")
_ACQUIRE_DELAY_SECONDS = 0.5
_MAX_COMMAND_BYTES = 256


class CognexError(Exception):
    """Base class for camera-link failures."""


class CognexConnectionError(CognexError):
    """The TCP session could not be opened or did not answer in time."""


class CognexProtocolError(CognexError):
    """The peer replied with a payload this service cannot use."""


class CognexReading:
    def __init__(self, raw_data: str):
        self.raw_data = raw_data.strip()
        parts = [p.strip() for p in self.raw_data.split(",")]
        
        if len(parts) < 4:
            raise CognexProtocolError(f"Payload incompleto desde B38: '{self.raw_data}'")

        self.exec_status = parts[0]          # "OK" o "ERR"
        self.step_ack = int(parts[1])        # 1, 2, etc.
        self.exec_time_ms = int(parts[2])    # ej. 2
        self.payload = parts[3:]             # ej. ["0", "0", "0", "0"] o ["NO_SWITCH"]
        self.status = self.exec_status       # Compatibilidad con tu código actual


def parse_reading(payload: str) -> CognexReading:
    """Parse ``1,PASS,ON,OFF,ON,ON,OFF`` into a structured reading."""
    raw = payload.strip()
    parts = raw.split(",")
    if len(parts) != 7:
        raise CognexProtocolError(
            f"Expected 7 comma-separated fields, received {raw!r}"
        )

    code, judgment, *positions = parts
    if code != "1":
        raise CognexProtocolError(f"Camera rejected the trigger: {raw!r}")
    if judgment not in {"PASS", "FAIL"}:
        raise CognexProtocolError(f"Unknown judgment {judgment!r} in {raw!r}")
    if any(position not in _SWITCH_STATES for position in positions):
        raise CognexProtocolError(f"Invalid DIP positions in {raw!r}")

    return CognexReading(
        passed=judgment == "PASS",
        dip_positions=positions,
        raw=raw,
    )


class CognexCameraMock:
    """Async TCP server that behaves like a single Cognex camera."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._server: asyncio.AbstractServer | None = None

    @property
    def is_serving(self) -> bool:
        return self._server is not None and self._server.is_serving()

    async def start(self) -> None:
        """Bind the Native Mode port. Raises ``OSError`` if the port is taken."""
        if self._server is not None:
            return
        self._server = await asyncio.start_server(
            self._handle_client,
            host=self.host,
            port=self.port,
            limit=_MAX_COMMAND_BYTES,
        )
        sockets = self._server.sockets or []
        bound = ", ".join(str(sock.getsockname()) for sock in sockets)
        logger.info("Cognex mock listening on %s", bound or f"{self.host}:{self.port}")

    async def stop(self) -> None:
        """Stop accepting connections and close the listening socket."""
        server = self._server
        self._server = None
        if server is None:
            return
        server.close()
        await server.wait_closed()
        logger.info("Cognex mock stopped")

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername")
        try:
            while True:
                try:
                    data = await asyncio.wait_for(reader.readline(), timeout=30.0)
                except asyncio.TimeoutError:
                    logger.info("Closing idle camera session from %s", peer)
                    break
                if not data:
                    break
                if len(data) > _MAX_COMMAND_BYTES:
                    writer.write(b"0,ERR,COMMAND_TOO_LONG\r\n")
                    await writer.drain()
                    break

                command = data.decode("ascii", errors="replace").strip()
                if not command:
                    continue
                response = await self._dispatch(command)
                writer.write(response.encode("ascii"))
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError, OSError) as exc:
            logger.warning("Camera session from %s ended: %s", peer, exc)
        except Exception:
            logger.exception("Unexpected failure in camera session from %s", peer)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass

    async def _dispatch(self, command: str) -> str:
        normalized = command.upper()
        if normalized in _TRIGGER_COMMANDS:
            await asyncio.sleep(_ACQUIRE_DELAY_SECONDS)
            return self._build_result()
        if normalized == "PING":
            return "1\r\n"
        logger.info("Rejected unknown camera command %r", command)
        return "0,ERR,UNKNOWN_COMMAND\r\n"

    @staticmethod
    def _build_result() -> str:
        """Build a realistic pass/fail line with five random DIP positions.

        About one in five inspections fails so the dashboard shows both states.
        The command itself still succeeds (leading ``1``).
        """
        judgment = "PASS" if random.random() >= 0.2 else "FAIL"
        positions = [random.choice(_SWITCH_STATES) for _ in range(5)]
        return ",".join(["1", judgment, *positions]) + "\r\n"


class CognexCameraClient:
    """Short-lived TCP client for trigger and health-probe commands."""

    def __init__(self, host: str, port: int, timeout: float) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self._lock = asyncio.Lock()

    async def trigger(self, command: str = "SW8", step_id: int = 1) -> CognexReading:
        """Inyecta el paso actual (SI) a la celda A3, dispara SW8 y lee B38."""
        try:


            # 1. Avisarle a la cámara en qué paso vamos (Celda A3)
            resp_si = await self._transact(f"SIA003 {step_id}", lines_to_read=1)
            if resp_si != "1":
                logger.warning(f"Set Integer A003 devolvió: {resp_si}")
            
            # 2. Tira el gatillo (SW8)
            await self._transact(command, lines_to_read=1)
            
            # 3. Leer la celda B38 (línea 1 = "1", línea 2 = "OK,1,2,0,0,0,0")
            raw_resp = await self._transact("GVB038", lines_to_read=2)
            lines = [line.strip() for line in raw_resp.splitlines() if line.strip()]
            
            if len(lines) < 2 or lines[0] != "1":
                raise CognexProtocolError(f"Error leyendo B38. Respuesta: {raw_resp}")
                
            return CognexReading(lines[1])
        except Exception as exc:
            raise CognexProtocolError(f"Falla en la secuencia de Trigger: {exc}")

    # async def probe(self) -> bool:
    #     """Return true when the camera accepts a TCP session and answers PING."""
    #     try:
    #         reply = (await self._transact("SO1")).strip()
    #     except CognexError:
    #         return False
    #     return reply == "1"
    async def probe(self) -> bool:
        if self._lock.locked():
            return True
        try:
            await self._transact("GV000", lines_to_read=1)
            return True
        except Exception:
            return False


    async def load_job(self, job_name: str) -> None:
        """Cambia el .jobx en una sola sesión TCP bloqueando el health-check."""
        async with self._lock:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port),
                    timeout=self.timeout,
                )
            except (OSError, asyncio.TimeoutError) as exc:
                raise CognexConnectionError(f"Camera at {self.host}:{self.port} is unreachable") from exc

            async def send(cmd: str, read_timeout: float = 10.0) -> str:
                writer.write(f"{cmd}\r\n".encode("ascii"))
                await writer.drain()
                data = await asyncio.wait_for(reader.readline(), timeout=read_timeout)
                return data.decode("ascii", errors="replace").strip()

            try:
                # 1. Un solo Login para toda la secuencia
                await asyncio.wait_for(reader.readuntil(b":"), timeout=self.timeout)
                writer.write(b"admin\r\n")
                await writer.drain()
                await asyncio.wait_for(reader.readuntil(b":"), timeout=self.timeout)
                writer.write(b"\r\n")
                await writer.drain()
                await asyncio.wait_for(reader.readline(), timeout=self.timeout)

                # 2. Offline
                resp_off = await send("SO0")
                if resp_off != "1":
                    raise CognexProtocolError(f"Fallo al poner Offline. Respuesta: {resp_off}")

                # 3. Cargar Job (dándole 15s por si el .jobx está pesado)
                resp_load = await send(f"LF{job_name}", read_timeout=15.0)
                logger.info(f"LF{job_name} -> {resp_load}")
                if resp_load == "-1":
                    raise CognexProtocolError(f"El archivo {job_name} NO EXISTE en la cámara.")
                elif resp_load == "-2":
                    raise CognexProtocolError(f"Sin memoria suficiente para cargar {job_name}.")
                elif resp_load != "1":
                    raise CognexProtocolError(f"Error al cargar {job_name}. Respuesta: {resp_load}")

                await asyncio.sleep(2.0)

                # 4. Levantar a Online (o confirmar con GO que ya está Online)
                for attempt in range(10):
                    resp_online = await send("SO1")
                    if resp_online == "1":
                        logger.info(f"Job {job_name} cargado y cámara Online.")
                        return
                    # Verificar si el Job ya levantó solo en Online
                    if await send("GO") == "1":
                        logger.info(f"Job {job_name} ya se encuentra Online (GO=1).")
                        return
                    logger.warning(f"SO1 devolvió {resp_online}. Reintento {attempt + 1} de 10...")
                    await asyncio.sleep(1.5)

                raise CognexProtocolError("Se agotaron los reintentos para poner la cámara Online.")
            finally:
                writer.close()
                try:
                    await writer.wait_closed()
                except (ConnectionError, OSError):
                    pass
        
    async def _transact(self, command: str, lines_to_read: int = 1) -> str:
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port),
                timeout=self.timeout,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            raise CognexConnectionError(f"Camera at {self.host}:{self.port} is unreachable") from exc

        try:
            # 1. Login Cognex (Usuario admin, sin password)
            await asyncio.wait_for(reader.readuntil(b":"), timeout=self.timeout)
            writer.write(b"admin\r\n")
            await writer.drain()
            
            await asyncio.wait_for(reader.readuntil(b":"), timeout=self.timeout)
            writer.write(b"\r\n")
            await writer.drain()
            
            await asyncio.wait_for(reader.readline(), timeout=self.timeout)

            # 2. Enviar el comando exacto que pide el dashboard (ej. SW8)
            writer.write(f"{command}\r\n".encode("ascii"))
            await writer.drain()
            
            lines: list[str] = []
            for _ in range(lines_to_read):
                data = await asyncio.wait_for(reader.readline(), timeout=self.timeout)
                if not data:
                    raise CognexProtocolError("Camera closed the connection without a response")
                lines.append(data.decode("ascii", errors="replace").strip())

            return "\n".join(lines)
            
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError):
                pass