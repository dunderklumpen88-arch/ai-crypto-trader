import csv
import io
from datetime import datetime, timezone


def create_tax_csv(trades):
    """
    Skapar CSV-underlag för granskning inför svensk K4.
    Belopp anges i SEK.
    Detta är ett underlag, inte en färdig K4-deklaration.
    """
    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")

    writer.writerow([
        "Datum",
        "Kryptovaluta",
        "Händelse",
        "Antal",
        "Försäljningsbelopp_SEK",
        "Omkostnadsbelopp_SEK",
        "Vinst_förlust_SEK",
        "Transaktions_id"
    ])

    for trade in trades:
        writer.writerow([
            trade.get("date", ""),
            trade.get("asset", ""),
            trade.get("type", ""),
            trade.get("quantity", 0),
            trade.get("proceeds_sek", 0),
            trade.get("cost_basis_sek", 0),
            trade.get("gain_loss_sek", 0),
            trade.get("transaction_id", "")
        ])

    return output.getvalue()


def utc_timestamp():
    return datetime.now(timezone.utc).isoformat()
