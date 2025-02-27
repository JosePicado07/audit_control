# Audit Control Project Guidelines

## Commands
- Run application: `python main.py`
- Setup environment: `python -m pip install -e .`
- Install GUI dependencies: `python -m pip install customtkinter Pillow`
- Run specific test: `python -m unittest tests.TestInventoryMatcher`
- Run all tests: `python -m unittest discover tests`
- Run integration tests: `python -m unittest discover tests.integration`
- Debug mode: `python debug.py`

## Code Style Guidelines
- **Imports**: Standard library first, then third-party packages, then local modules
- **Documentation**: Docstrings in Spanish using """triple quotes""" with Args/Returns sections
- **Types**: Use typing annotations for all function parameters and return values
- **Naming**: 
  - Classes: PascalCase
  - Functions/variables: snake_case
  - Constants: UPPER_SNAKE_CASE
- **Structure**: Follow Clean Architecture (domain, application, infrastructure, presentation)
- **Error Handling**: Use try/except with specific exceptions and detailed error messages
- **Logging**: Use the centralized logger via `from infrastructure.logging.logger import get_logger`
- **File Access**: Use pathlib.Path instead of string paths
- **Data Validation**: Implement validation in entity __post_init__ methods