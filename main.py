from utils.api_requets import get_api_request, load_api_data, save_api_data
from utils.csv_reader import read_transaction_data
from utils.database_connection import DatabaseConnection


def main():
    """
    User data from :
        https://randomuser.me/

    Transaction data from:
        https://www.kaggle.com/datasets/computingvictor/transactions-fraud-datasets?select=transactions_data.csv

    """

    """
    # store data from api locally for testing
    save_api_data("data/api_data.pkl", api_data)
    api_data = load_api_data("data/api_data.pkl")
    """

    # TODO: Put args in config file
    api_url = "https://randomuser.me/api"
    sql_args = {"recreate_database": True, "database_name": "southwind"}
    transaction_args = {
        "filepath": "data/transactions_data.csv",
        "row_limit": 1000000,
        "columns": {
            "id": int,
            "date": str,
            "client_id": int,
            "merchant_id": int,
            "amount": str,
        },
    }

    with DatabaseConnection(**sql_args) as database:
        api_data = get_api_request(api_url)
        database.write_users(api_data)

        transaction_data = read_transaction_data(**transaction_args)
        database.write_transactions(transaction_data)


if __name__ == "__main__":
    main()
