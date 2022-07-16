from beancount.core.number import D
from beancount.ingest import importer
from beancount.core import account
from beancount.core import amount
from beancount.core import flags
from beancount.core import data

from dateutil.parser import parse
from datetime import datetime, timedelta
from contextlib import contextmanager

import csv
import os

from . import lang

@contextmanager
def csv_open(filename):
    with open(filename, newline='', encoding='utf-8-sig') as f:
        yield csv.DictReader(f, quotechar='"')


class PaypalImporter(importer.ImporterProtocol):
    def __init__(
        self,
        email_address,
        account,
        checking_account,
        commission_account,
        language=None,
        metadata_map=None,
        keep_empty_metadata=True,
        categorize=None
    ):
        if language is None:
            language = lang.en()

        if metadata_map is None:
            metadata_map = language.metadata_map

        self.email_address = set(email_address)
        self.account = account
        self.checking_account = checking_account
        self.commission_account = commission_account
        self.language = language
        self.metadata_map = metadata_map
        self.keep_empty_metadata = keep_empty_metadata
        self.categorize = categorize

    def file_account(self, _):
        return self.account

    def identify(self, filename):
        with csv_open(filename.name) as rows:
            try:
                row = next(rows)
                if not self.language.identify(list(next(rows).keys())):
                    return False

                row = self.language.normalize_keys(row)
                if not (row['from'] in self.email_address or row['to'] in self.email_address):
                    return False

                return True
            except (StopIteration, UnicodeDecodeError):
                return False

    def _categorize_payment(self, txn, raw_row, value, currency):
        if self.categorize:
            category = self.categorize(raw_row)
            if category:
                txn.postings.append(
                    data.Posting(
                        category,
                        amount.Amount(-1*D(value), currency),
                        None, None, None, None
                    )
                )

    def extract(self, filename):
        entries = []
        last_txn_id = None
        last_net = None
        last_currency = None
        last_was_currency = False

        with csv_open(filename.name) as rows:
            for index, raw_row in enumerate(rows):
                row = self.language.normalize_keys(raw_row)

                # Disregard entries about invoices being sent. Merely sending
                # an invoice doesn't affect balances, only their payment does,
                # which will show up in a separate row.
                if self.language.txn_invoice_sent(row):
                    continue

                metadata = { k: raw_row[v] for k, v in self.metadata_map.items()
                        if self.keep_empty_metadata or raw_row.get(v) }

                row['date'] = self.language.parse_date(row['date']).date()
                row['gross'] = self.language.decimal(row['gross'])
                row['fee'] = self.language.decimal(row['fee'])
                row['net'] = self.language.decimal(row['net'])

                if row['reference_txn_id'] != last_txn_id:
                    meta = data.new_metadata(filename.name, index, metadata)

                    txn = data.Transaction(
                        meta=meta,
                        date=row['date'],
                        flag=flags.FLAG_OKAY,
                        payee=row['name'],
                        narration=row.get('item_title') or row.get('subject') or row.get('note'),
                        tags=set(),
                        links=set(),
                        postings=[],
                    )

                if self.language.txn_from_checking(row):
                    txn.postings.append(
                        data.Posting(
                            self.checking_account,
                            amount.Amount(-1*D(row['gross']), row['currency']),
                            None, None, None, None
                        )
                    )

                    txn.postings.append(
                        data.Posting(
                            self.account,
                            amount.Amount(D(row['net']), row['currency']),
                            None, None, None, None
                        )
                    )

                elif self.language.txn_to_checking(row):
                    txn.postings.append(
                        data.Posting(
                            self.account,
                            amount.Amount(D(row['gross']), row['currency']),
                            None, None, None, None
                        )
                    )

                    txn.postings.append(
                        data.Posting(
                            self.checking_account,
                            amount.Amount(-1*D(row['net']), row['currency']),
                            None, None, None, None
                        )
                    )

                elif self.language.txn_currency_conversion(row):
                    if last_was_currency:
                        txn.postings.append(
                            data.Posting(
                                self.account,
                                amount.Amount(D(last_net), last_currency),
                                None, None, None, None
                            )
                        )
                        txn.postings.append(
                            data.Posting(
                                self.account,
                                amount.Amount(D(row['net']), row['currency']),
                                None,
                                amount.Amount(-1*(D(last_net) / D(row['net'])), last_currency),
                                None, None
                            )
                        )
                        last_net = None
                        last_currency = None
                        last_was_currency = False
                    else:
                        last_net = row['net']
                        last_currency = row['currency']
                        last_was_currency = True

                elif row['to'] in self.email_address:
                    txn.postings.append(
                        data.Posting(
                            self.account,
                            amount.Amount(D(row['net']), row['currency']),
                            None, None, None, None
                        )
                    )
                    self._categorize_payment(txn, raw_row, row['gross'], row['currency'])

                else:
                    txn.postings.append(
                        data.Posting(
                            self.account,
                            amount.Amount(D(row['gross']), row['currency']),
                            None, None, None, None
                        )
                    )
                    self._categorize_payment(txn, raw_row, row['net'], row['currency'])

                if D(row['fee']) != 0:
                    txn.postings.append(
                        data.Posting(
                            self.commission_account,
                            amount.Amount(abs(D(row['fee'])), row['currency']),
                            None, None, None, None
                        )
                    )

                if row['reference_txn_id'] != last_txn_id:
                    entries.append(txn)
                    last_txn_id = row['txn_id']

                last_currency = row['currency']
                last_amount = amount

        if 'balance' in row:
            meta = data.new_metadata(filename.name, index + 1)
            entries.append(
                data.Balance(
                    meta,
                    row['date'] + timedelta(days=1),
                    self.account,
                    amount.Amount(D(self.language.decimal(row['balance'])), row['currency']),
                    None,
                    None,
                )
            )

        return entries
