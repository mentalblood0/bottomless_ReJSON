# <span style="color:gold">⚿</span> bottomless_ReJSON 

Library for seamless Redis database management. This version uses [RedisJSON](https://oss.redis.com/redisjson/) module

<br/>

* 💤 No excess data reading/rewriting
* 🗃 Integrated indexes support
* ⛓ Thread-safe operations
* 👁️ One-class interface
* 🪄 A lot of sugar

<br/>

## 💿 Installation

```bash
python -m pip install bottomless_ReJSON
```

<br/>

## ✒️ Usage

Here are just a few examples

Feel free to use the tests as a manual

### Preparations

```python
import bottomless_ReJSON.RedisInterface as RedisInterface

db = RedisInterface(host='localhost', port='6379')
```

### Dictionary-like interface

```python
db.clear()
d = {
    '1': {
        '1': {
            '1': 'one.one.one'
        },
        '2': 'one.two'
    },
    '2': 'two'
}
db |= d
assert db() == d

db['2'] = d
assert db['2']() == d

db['1']['1'] = 'lalala'
assert db['1']['1'] == 'lalala'
assert db['1']['1']['1'] == None
```

### List-like interface

```python
db.clear()
db['key'] = []
db = db['key']

l = [1, 2, 3]
db += [1, 2, 3]

i = 0
for e in db:
    # e is RedisInterface instance, 
    # so to get data you need to call it:
    assert e() == l[i]
    assert e() == db[i]
    i += 1

    assert list(db) == [1, 2, 3]
```

<br/>

## 🔬 Testing

```bash
git clone https://github.com/MentalBlood/bottomless_ReJSON
cd bottomless_ReJSON
pytest tests
```

<br/>

## 📈 Benchmarking

Using [sharpener](https://github.com/MentalBlood/sharpener):
```bash
git clone https://github.com/MentalBlood/bottomless_ReJSON
cd bottomless_ReJSON
py -m sharpener run
```
Config for benchmarks can be found in `benchmarks/benchmark_default.json`