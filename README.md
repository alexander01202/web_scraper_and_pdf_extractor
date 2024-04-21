# Python Scraper
This repository contains the barebones to setup a Python scraper.

:warning: **If the resource you are scraping requires you to agree to any Terms & Conditions, please do not proceed and notify your contract manager immediately.  Under no circumstances should you create a false account or fake identity.**

## Developing

### Project Structure
- `scraper/` - Place all your source code in this directory.
  - `scraper.py` - Main scraper code. You can treat the `run_scrape` function as the entrypoint and write your code here.
  - `__main__.py` - Main entrypoint to the scaper. This will be invoked with an output `$filename`, as in `python -m scraper $JSON_FILE`.
- `requirement.txt` - List of package requirements for your code to run. You can modify these to your needs.
- `sample.json` - A sample output from your scraper.

__Note__: You may also upload additional binaries or files and reference them as you need but please do not modify any of the other existing files (e.g. Dockerfile, run.sh, etc.).

### Environment
Install the necessary requirements into your Python environment. The command below will install the necessary `idr-requirements.txt` as well as your custom requirements.
```bash
$ pip install -r requirements.txt
```

__Note__: Make sure you re-run this when you add a new requirement to the `requirements.txt` file.

## Running
To run your code, you must invoke the scraper module with a filename argument. This can be done using the `-m` option with Python interpreter.
```bash
$ python -m scraper <filename>
```

## Testing
### Manual
You can manually test your code by running the `run.sh` script in your terminal. This will invoke your scraper with a random filename and output a summary.
```bash
$ ./run.sh
```
### Manual, Python Only
A subset of the tests can be run as follows: 
```
python -m scraper sample.json
python test_output.py sample.json
```


### Automated
Any commits to the main branch will automatically trigger a GitHub Actions workflow. This will build and test your code in a containerized environment. The tests must pass for your code to be accepted.

## Runtime
During the build process, the contents of this repository will be copied to `/usr/src/scrape`. Your code must be able to run from this path.
