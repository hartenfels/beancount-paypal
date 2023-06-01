from beancount.core.number import D, round_to
from beancount.ingest import importer
from beancount.core.amount import Amount
from beancount.core.flags import FLAG_OKAY
from beancount.core.data import Balance, Posting, Transaction, new_metadata

from dateutil.parser import parse
from datetime import datetime, timedelta
from contextlib import contextmanager

import csv
import re
import os

from . import lang


@contextmanager
def csv_open(filename):
    with open(filename, newline="", encoding="utf-8-sig") as f:
        yield csv.DictReader(f, quotechar='"')


def get_narration(row):
    return row.get("subject") or row.get("note") or row.get("item_title") or None


class EntryPosting:
    def __init__(self, kind, account, amount, currency):
        self.kind = kind
        self.account = account
        self.amount = amount
        self.currency = currency


class Entry:
    def __init__(self, importer, txn_id, name, index, date, payee, narration, kind):
        self.importer = importer
        self.txn_id = txn_id
        self.name = name
        self.index = index
        self.date = date
        self.payee = payee
        self.narration = narration
        self.kind = kind
        self.postings = []
        self.conversions = []
        self.gross = None
        self.txn_ids = [txn_id]
        self.parties = set()
        self.invoice_numbers = set()

    def add(self, kind, account, amount, currency):
        self.postings.append(EntryPosting(kind, account, amount, currency))

    def add_conversion(self, row):
        self.conversions.append(row)

    def add_metadata(self, row):
        txn_id = row["txn_id"]
        if not txn_id in self.txn_ids:
            self.txn_ids.append(txn_id)

        for party in [row["from"], row["to"]]:
            if party and party not in self.importer.email_address:
                self.parties.add(party)

        invoice_number = row.get("invoice_number")
        if invoice_number:
            self.invoice_numbers.add(invoice_number)

    def get_metadata(self):
        metadata = {}
        if self.kind:
            metadata["type"] = self.kind
        for i, txn_id in enumerate(self.txn_ids):
            metadata["txnid{}".format("" if i == 0 else i + 1)] = txn_id
        for i, party in enumerate(sorted(self.parties)):
            metadata["party{}".format("" if i == 0 else i + 1)] = party
        for i, invoice_number in enumerate(sorted(self.invoice_numbers)):
            metadata["invoiceno{}".format("" if i == 0 else i + 1)] = invoice_number
        return metadata

    def _get_currency(self):
        currencies = set(posting.currency for posting in self.postings)
        if len(currencies) == 1:
            return currencies.pop()
        else:
            raise ValueError(
                "Transaction {} doesn't have a single currency: {}".format(
                    self.txn_id, currencies
                )
            )

    def _get_total(self, negative):
        total = D(0)
        for posting in self.postings:
            if posting.kind != "fee":
                total += abs(posting.amount)
        return -total if negative else total

    def _make_convert(self, c1, c2):
        currency = self._get_currency()
        if c1["currency"] == currency and c2["currency"] != currency:
            foreign, own = c1, c2
        elif c1["currency"] != currency and c2["currency"] == currency:
            foreign, own = c2, c1
        else:
            raise ValueError(
                "Transaction {} has invalid conversion currencies {}".format(
                    self.txn_id, self.conversions
                )
            )

        foreign_amount = D(foreign["gross"])
        actual_amount = self._get_total(foreign_amount < D(0))
        if foreign_amount != actual_amount:
            raise ValueError(
                "Transaction {} has invalid conversion amount {} != {}".format(
                    self.txn_id, foreign_amount, actual_amount
                )
            )

        own_amount = abs(D(own["gross"]))
        own_currency = own["currency"]
        rate = own_amount / abs(foreign_amount)

        def convert(amount, _):
            converted = amount * rate
            if abs(abs(converted) - own_amount) <= 0.001:
                amount = -own_amount if converted < D(0) else own_amount
            else:
                amount = int(converted * D(100) + D(0.5)) / D(100)
            return Amount(amount, own_currency)

        return convert

        return lambda amount, _: Amount(amount * rate, own_currency)

    def get_currency_conversion(self):
        if len(self.conversions) == 0:
            return lambda amount, currency: Amount(amount, currency)
        elif len(self.conversions) == 2:
            return self._make_convert(*self.conversions)
        else:
            raise ValueError(
                "Transaction {} has {} conversion posting(s)".format(
                    self.txn_id, len(self.conversions)
                )
            )


class ExtractState:
    def __init__(self, importer, filename):
        self._importer = importer
        self._name = filename.name
        self._entries = []
        self._entries_by_txn_id = {}
        self._last_balance_row = None

    def _handle_fee(self, row, entry):
        fee = D(row["fee"])
        if fee != 0:
            entry.add(
                "fee", self._importer.commission_account, abs(fee), row["currency"]
            )

    def _handle_from_checking(self, row, entry):
        currency = row["currency"]
        entry.add_metadata(row)
        entry.add(
            "from_checking",
            self._importer.checking_account,
            -1 * D(row["gross"]),
            currency,
        )
        entry.add("to_paypal", self._importer.account, D(row["net"]), currency)
        self._handle_fee(row, entry)

    def _handle_to_checking(self, row, entry):
        currency = row["currency"]
        entry.add_metadata(row)
        entry.add("from_paypal", self._importer.account, D(row["gross"]), currency)
        entry.add(
            "to_checking", self._importer.checking_account, -1 * D(row["net"]), currency
        )
        self._handle_fee(row, entry)

    def _handle_receive(self, row, entry):
        entry.narration = get_narration(row)
        entry.add_metadata(row)
        entry.add("receive", self._importer.account, D(row["net"]), row["currency"])
        self._handle_fee(row, entry)

    def _handle_send(self, row, entry):
        entry.narration = get_narration(row)
        entry.add_metadata(row)
        entry.add("send", self._importer.account, D(row["net"]), row["currency"])
        self._handle_fee(row, entry)

    def _handle_currency_conversion(self, row, entry):
        entry.add_conversion(row)

    def _handle_row(self, index, row, entry):
        language = self._importer.language
        # Memos don't affect the balance.
        if not language.txn_memo(row):
            # Currency conversions must be buffered for later. Transactions that
            # seem to have multiple postings in the CSV just get their first one
            # handled, because the remaining ones are just internal movements.
            if language.txn_currency_conversion(row):
                self._handle_currency_conversion(row, entry)
            elif not entry.postings:
                if language.txn_from_checking(row):
                    self._handle_from_checking(row, entry)
                elif language.txn_to_checking(row):
                    self._handle_to_checking(row, entry)
                elif row["to"] in self._importer.email_address:
                    self._handle_receive(row, entry)
                else:
                    self._handle_send(row, entry)

    def _handle_new_row(self, index, row, kind):
        entry = Entry(
            importer=self._importer,
            txn_id=row["txn_id"],
            name=self._name,
            index=index,
            date=row["date"],
            payee=row["name"],
            narration=get_narration(row),
            kind=kind,
        )

        self._handle_row(index, row, entry)
        return entry

    def _handle_reference_row(self, index, row, entry):
        self._handle_row(index, row, entry)

    def _extract_row(self, index, row):
        txn_id = row["txn_id"]
        ref_id = row["reference_txn_id"]
        language = self._importer.language
        entry = not language.txn_refund(row) and (
            self._entries_by_txn_id.get(ref_id) or self._entries_by_txn_id.get(txn_id)
        )
        if entry:
            self._handle_reference_row(index, row, entry)
            self._entries_by_txn_id[txn_id] = entry
        else:
            entry = self._handle_new_row(index, row, language.txn_kind(row))
            self._entries.append(entry)

        self._entries_by_txn_id[txn_id] = entry
        if ref_id:
            self._entries_by_txn_id[ref_id] = entry

    def extract(self, index, raw_row):
        language = self._importer.language
        row = language.normalize_keys(raw_row)
        row["date"] = language.parse_date(row["date"]).date()
        row["gross"] = language.decimal(row["gross"])
        row["fee"] = language.decimal(row["fee"])
        row["net"] = language.decimal(row["net"])

        self._extract_row(index, row)

        if "balance" in row:
            self._last_balance_row = (index, row)

    def _mangle_entry(self, entry):
        txn = Transaction(
            meta=new_metadata(entry.name, entry.index, entry.get_metadata()),
            date=entry.date,
            flag=FLAG_OKAY,
            payee=entry.payee,
            narration=entry.narration,
            tags=set(),
            links=set(),
            postings=[],
        )

        convert = entry.get_currency_conversion()
        for posting in entry.postings:
            txn.postings.append(
                Posting(
                    account=posting.account,
                    units=convert(posting.amount, posting.currency),
                    cost=None,
                    price=None,
                    flag=None,
                    meta=None,
                )
            )

        return txn

    def finish(self):
        if self._importer.pre_process:
            self._importer.pre_process(self._entries)

        entries = [self._mangle_entry(entry) for entry in self._entries]

        if self._last_balance_row:
            language = self._importer.language
            index, row = self._last_balance_row
            balance = Balance(
                new_metadata(self._name, index + 1),
                row["date"] + timedelta(days=1),
                self._importer.account,
                Amount(D(language.decimal(row["balance"])), row["currency"]),
                None,
                None,
            )
            if self._importer.post_process:
                self._importer.post_process(balance)
            entries.append(balance)

        if self._importer.post_process:
            self._importer.post_process(entries)

        return entries


class PaypalImporter(importer.ImporterProtocol):
    def __init__(
        self,
        email_address,
        account,
        checking_account,
        commission_account,
        language=None,
        pre_process=None,
        post_process=None,
    ):
        if language is None:
            language = lang.en()

        self.email_address = set(email_address)
        self.account = account
        self.checking_account = checking_account
        self.commission_account = commission_account
        self.language = language
        self.pre_process = pre_process
        self.post_process = post_process

    def file_account(self, _):
        return self.account

    def identify(self, filename):
        with csv_open(filename.name) as rows:
            try:
                row = next(rows)
                if not self.language.identify(list(next(rows).keys())):
                    return False

                row = self.language.normalize_keys(row)
                if not (
                    row["from"] in self.email_address or row["to"] in self.email_address
                ):
                    return False

                return True
            except (StopIteration, UnicodeDecodeError):
                return False

    def extract(self, filename):
        state = ExtractState(self, filename)
        with csv_open(filename.name) as rows:
            for index, raw_row in enumerate(rows):
                state.extract(index, raw_row)
        return state.finish()
