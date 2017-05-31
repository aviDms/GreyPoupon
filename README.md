# GreyPoupon
## Python wrapper of the GoodData's REST API.

## Development

https://packaging.python.org/distributing/

### How to synchronise a master project with a slave project

First, create a new folder in which to start a new python virtul environment:

```bash
mkdir test_grey_poupon
cd test_grey_poupon
python3 -m venv .venv
source .venv/bin/activate
```


Second, install in the new .venv grey_poupon from github directly:
```bash
pip install -e git+git://github.com/aviDms/GreyPoupon.git#egg=GreyPoupon
```

Now you should be able to use gp command line tool to create and save a login
file on your machine and a configuration file:

```bash
gp --auth
gp --config
```

Follow the command prompts and make sure to provide the correct arguments.

!!! The --auth and --config should be run only once, at setup.

Once the config files are created you can run the automatic sync:

```bash
gp --sync
```