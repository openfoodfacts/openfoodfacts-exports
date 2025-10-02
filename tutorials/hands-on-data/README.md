# Open Food Facts data tutorial

Hands on Open Food Facts data with a tutorial that you can play around with.

## Install

If you want to play around the Notebook, you may use Jupyter Lab (or any other Notebook capable frontend).

```bash
# create a activate a virtual environment
python3 -m venv venv
. venv/bin/activate

# install jupyter lab, and some extensions
pip install -r requirements.txt

# launch it:
jupyter lab
```

## Adding a Hugging Face token

Go to [hugging face](https://huggingface.co), log-in (eventually creating an account),
and create a new token.
Put the content in a file named `hf-token` at the root of this project.
