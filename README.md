- Get all [stocks finfo](https://finfo-api.vndirect.com.vn/v4/stocks?q=status:listed&size=10000&page=) and save
  in `data/prepare/stocks_finfo.json`
- Run file `python3 ./data/prepare/get_stocks_info.py` to filter stock from stocks finfo
- Restart status crawl: `echo '{"1D": [], "1": []}' > ./data/success_symbols.json`
- Update SERVER_URL in constant.py
- Start crawl: `python3 main.py`