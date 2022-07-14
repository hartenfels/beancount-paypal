from datetime import datetime

class base():
    # Paypal calls these "T-Codes", they identify a transaction type in a
    # language-agnostic way. See their developer documentation for a list.
    _txn_codes_from_checking = set(["T0300"])
    _txn_codes_to_checking = set(["T0400"])
    _txn_codes_currency_conversion = set(["T0200", "T0201", "T0202"])
    _txn_codes_invoice_sent = set(["T7101"])

    def identify(self, fields):
        return all(elem in fields for elem in list(self.fields_map.keys())[:-5])  # last 5 keys are optional

    def _is_type(self, row, codes, title):
        return row.get("txn_code") in codes or row["txn_type"] == title

    def txn_from_checking(self, row):
        return self._is_type(row, self._txn_codes_from_checking, self._from_checking)

    def txn_to_checking(self, row):
        return self._is_type(row, self._txn_codes_to_checking, self._to_checking)

    def txn_currency_conversion(self, row):
        return self._is_type(row, self._txn_codes_currency_conversion, self._currency_conversion)

    def txn_invoice_sent(self, row):
        return self._is_type(row, self._txn_codes_invoice_sent, self._invoice_sent)

    def decimal(self, data):
        return data

    def parse_date(self, data):
        return datetime.strptime(data, self._format)

    def normalize_keys(self, row):
        return { self.fields_map.get(k, k):row[k] for k in row }


class en(base):
    fields_map = {
        "Date": "date",
        "Time": "time",
        "TimeZone": "timezone",
        "Name": "name",
        "Type": "txn_type",
        "Status": "status",
        "Currency": "currency",
        "Gross": "gross",
        "Fee": "fee",
        "Net": "net",
        "From Email Address": "from",
        "To Email Address": "to",
        "Transaction ID": "txn_id",
        "Reference Txn ID": "reference_txn_id",
        "Receipt ID": "receipt_id",
        # Optional keys:
        "Item Title": "item_title",
        "Subject": "subject",
        "Note": "note",
        "Balance": "balance",
        "Transaction Event Code": "txn_code",
    }

    metadata_map = {
        "uuid": "Transaction ID",
        "sender": "From Email Address",
        "recipient": "To Email Address",
    }

    _format = "%d/%m/%Y"
    _from_checking = "Bank Deposit to PP Account "
    _to_checking = "General Withdrawal - Bank Transfer"
    _currency_conversion = "General Currency Conversion"
    _invoice_sent = "Invoice Sent"

    def decimal(self, data):
        return data.replace(".", "").replace(",", ".")


class de(base):
    fields_map = {
        "Datum": "date",
        "Uhrzeit": "time",
        "Zeitzone": "timezone",
        "Name": "name",
        "Typ": "txn_type",
        "Status": "status",
        "Währung": "currency",
        "Brutto": "gross",
        "Gebühr": "fee",
        "Netto": "net",
        "Absender E-Mail-Adresse": "from",
        "Empfänger E-Mail-Adresse": "to",
        "Transaktionscode": "txn_id",
        "Zugehöriger Transaktionscode": "reference_txn_id",
        "Empfangsnummer": "receipt_id",
        # Optional keys:
        "Artikelbezeichnung": "item_title",
        "Betreff": "subject",
        "Hinweis": "note",
        "Guthaben": "balance",
        "Transaktionsereigniscode": "txn_code",
    }

    metadata_map = {
        "uuid": "Transaktionscode",
        "sender": "Absender E-Mail-Adresse",
        "recipient": "Empfänger E-Mail-Adresse",
    }

    _format = "%d.%m.%Y"
    _from_checking = "Bankgutschrift auf PayPal-Konto"
    _to_checking = "Allgemeine Abbuchung"
    _currency_conversion = "Allgemeine Währungsumrechnung"
    _invoice_sent = "Rechnung gesendet"

    def decimal(self, data):
        return data.replace(".", "").replace(",", ".")
