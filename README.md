# Installation

The core functionality only relies on my other repository [sew](https://github.com/icyveins7/sew). Head over there to see how best to install it.

## Telegram Bot
To use the telegram bot functionality, you must also 

```bash
pip install -r requirements.txt
```

Be sure to clone with submodules to use the telegram bot:

```bash
git clone https://github.com/icyveins7/tledb
cd tledb
git submodule update --init --recursive
```

If there are changes to the submodule, you may need to

```bash
git submodule update --remote
```