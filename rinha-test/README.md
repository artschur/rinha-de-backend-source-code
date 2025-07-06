
Build k6 with extensions (not used)
```shell 
docker run --rm -u "$(id -u):$(id -g)" -v "${PWD}:/xk6" \
    grafana/xk6 build \
    --with github.com/grafana/xk6-redis \
    --with github.com/faunists/xk6-fs \
    --with github.com/szkiba/xk6-csv
```

k6 dashboard
```shell
export K6_WEB_DASHBOARD=true
export K6_WEB_DASHBOARD_PORT=5665
export K6_WEB_DASHBOARD_PERIOD=2s
export K6_WEB_DASHBOARD_OPEN=true
export K6_WEB_DASHBOARD_EXPORT='results/report.html'
```

run test
```shell
k6 run -e MAX_REQUESTS=850 rinha.js && xdg-open results/report.html
```



generate payments.csv (not used)
```python
import random
import uuid

rows = 100

with open('payments.csv', 'w') as file:
    row = f"correlationId,amount\n"
    file.write(row)
    for _ in range(rows):
        id = str(uuid.uuid4())
        amount = round((random.random() + 1) * 1000, 2)
        row = f"{id},{amount}\n"
        file.write(row)

```