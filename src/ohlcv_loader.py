import requests, time
import pykx as kx

class BinanceOhlcvLoader:
    def __init__(self, symbol: str = 'BTCUSDT', interval: str = '1d') -> None:
        """
        Initialize the loader with symbol (BTCUSDT, SOLUSDT... etc), interval and timestamp of final candlestick in the 1000 you are looking for.
        ### Parameters:
        **symbol (str):** The trading pair symbol.<br>
        **interval (str):** The interval for the candlesticks ('1s', '1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M').<br>
        """
        self.symbol = symbol
        self.interval = interval
        self.base_url = 'https://www.binance.com/api/v3/uiKlines?'
        self.max_limit = 1000

        # self.params = {
        #     'endTime': str(end_time),
        #     'limit': str(self.max_limit),
        #     'symbol': self.symbol,
        #     'interval': self.interval,
        # }
    
        self.headers = {
            'accept': '*/*',
            'accept-language': 'en-GB,en;q=0.9,fa-IR;q=0.8,fa;q=0.7,en-US;q=0.6',
            'cache-control': 'no-cache',
            'clienttype': 'web',
            'content-type': 'application/json',
            'lang': 'en-GB',
            'pragma': 'no-cache',
            'priority': 'u=1, i',
            'referer': 'https://www.binance.com/en-GB/trade/SOL_USDT?type=spot',
            'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'x-passthrough-token': '',
        }

        self.cookies = {}
    def save_to_kdb(self, data: list, kdb_table: str = None) -> None:
        """
        Save the fetched OHLCV data to a kdb+ database for efficient querying and analysis.
        ### Parameters:
        **data (list):** The OHLCV data to be saved.
        """
        if not data:
            print("No data to save.")
            return
        
        # Convert the data into a kdb+ table
        ohlcv_table = kx.q('([] timestamp:(); open:(); high:(); low:(); close:(); volume:())')
        
        for entry in data:
            timestamp, open_price, high_price, low_price, close_price, volume = entry[:6]
            ohlcv_table = ohlcv_table.upsert(kx.q(f'({timestamp};{open_price};{high_price};{low_price};{close_price};{volume})'))
        
        # Save the table to a kdb+ database file
        db_filename = f"{self.symbol}_{self.interval}_ohlcv.q"
        kx.q(f"`:{db_filename} set {ohlcv_table}")
        print(f"Data saved to {db_filename}")
    def fetch_ohlcv(self, recursive: bool = False, until: int = None) -> list:
        """
        Fetch OHLCV data from the Binance API. If recursive is True, it will fetch data recursively until the 'until' timestamp is reached.<br>
        If 'until' is None, it will fetch the most recent batch of data. <br>
        The data will be stored in kdb+ databases for efficient querying and analysis for recursive calls.<br>
        ## Parameters:
        **recursive (bool)**: Whether to fetch data recursively. If set to True and 'until' is None, it will fetch from the most recent data backwards, until the earliest available data.<br>
        **until (int)**: The timestamp until which to fetch data. If None, and recursive = False, it will fetch the most recent data.<br>
        """
        if recursive == False:
            if until is None:
                params = {
                    'limit': str(self.max_limit),
                    'symbol': self.symbol,
                    'interval': self.interval,
                }
            else:
                params = {
                    'endTime': int(until),
                    'limit': str(self.max_limit),
                    'symbol': self.symbol,
                    'interval': self.interval,
                }
            try:
                response = requests.get(self.base_url, params=params, headers=self.headers, cookies=self.cookies)
                return response.json()
            except Exception as e:
                print(f"Error fetching data: {e}")
                return []
        
        if until == None:
            until = 0
        initial_params = {
            'limit': str(self.max_limit),
            'symbol': self.symbol,
            'interval': self.interval,
        }
        initial_response = requests.get(self.base_url, params=initial_params, headers=self.headers, cookies=self.cookies)
        response_len = len(initial_response.json())
        next_timestamp = initial_response.json()[0][0] - 1
        while response_len == 1000 or next_timestamp > until:
            



            
print(BinanceOhlcvLoader().fetch_ohlcv(recursive=True))