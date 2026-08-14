"""Shared recording fakes for KMA Bronze persistence tests."""


class RecordingCursor:
    def __init__(self):
        self.statements = []

    def execute(self, sql):
        self.statements.append(" ".join(sql.split()))


class VerificationCursor(RecordingCursor):
    def __init__(self, row):
        super().__init__()
        self.row = row

    def fetchone(self):
        return self.row


class RecordingTransaction:
    def __init__(self):
        self.events = []
        self.commits = 0

    def __enter__(self):
        self.events.append(("enter", None))
        return self

    def __exit__(self, exc_type, _exc, _tb):
        if exc_type is None:
            self.commits += 1
        self.events.append(("exit", exc_type))

    def delete(self, predicate):
        self.events.append(("delete", predicate))

    def append(self, arrow_table):
        self.events.append(("append", arrow_table))


class RecordingTable:
    def __init__(self):
        self.txn = RecordingTransaction()

    def transaction(self):
        return self.txn
