from datetime import datetime


class ServerDateTime:
    """
    Helper for generating consistent server-side UTC timestamps in milliseconds.
    """
    @staticmethod
    def now_epoch_ms() -> int:
        """Return current UTC time as milliseconds since epoch."""
        return int(datetime.utcnow().timestamp() * 1000)
