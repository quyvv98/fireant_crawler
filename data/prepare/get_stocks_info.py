import json

f = open('stocks_finfo.json')
data_crawled = json.load(f)
stocks = []
for d in data_crawled['data']:
    if d['type'] == 'STOCK':
        stocks.append(d)
        print("Added", d['code'])

print("Detected", len(stocks), 'symbols')
# Writing to sample.json
json_object = json.dumps(stocks, indent=4)
with open("../stocks.json", "w") as outfile:
    outfile.write(json_object)
