# Amazon Stock Data to Elasticsearch

This project includes a Python script to read Amazon stock data from a CSV file and insert it into Elasticsearch.

## Prerequisites

- macOS
- Python 3.9 or higher
- pip (Python package installer)

## Setup Instructions

### 1. Install venv (Virtual Environment)

macOS comes with Python pre-installed, which includes venv. You don't need to install it separately.

### 2. Create a Virtual Environment

Open Terminal and navigate to your project directory:

```bash
cd /path/to/your/project
```

Create a new virtual environment:

```bash
python3 -m venv myenv
```

This creates a new directory called `myenv` in your project folder.

### 3. Activate the Virtual Environment

Activate the virtual environment:

```bash
source myenv/bin/activate
```

Your terminal prompt should now start with `(myenv)`, indicating that the virtual environment is active.

### 4. Install Dependencies

With the virtual environment activated, install the required packages:

```bash
pip install -r requirements.txt
```

## Running the Script

1. Ensure your Elasticsearch instance is running locally on the default port (9200).

2. Make sure your CSV file is in the same directory as the script.

3. Run the script:

```bash
python csv_to_elasticsearch.py
```

## Deactivating the Virtual Environment

When you're done working on the project, you can deactivate the virtual environment:

```bash
deactivate
```

## Troubleshooting

If you encounter any issues with permissions when creating the virtual environment, you may need to use `sudo`:

```bash
sudo python3 -m venv myenv
```

Then change the ownership of the virtual environment directory:

```bash
sudo chown -R $(whoami) myenv
```

## Additional Notes

- Remember to activate the virtual environment every time you work on this project.
- If you add new dependencies to your project, update the `requirements.txt` file:

  ```bash
  pip freeze > requirements.txt
  ```

- To delete the virtual environment, simply delete the `myenv` directory.