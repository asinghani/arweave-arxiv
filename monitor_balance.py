from webhook import report_balance
from time import sleep
from arweave import Wallet

wallet = Wallet("DevWallet")

while True:
    report_balance(wallet.get_balance())

    sleep(60)
