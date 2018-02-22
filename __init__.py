from airflow.plugins_manager import AirflowPlugin
from variable_connection_plugin.operators.variable_connection_operator import CreateConnectionsFromVariable


class CreateConnectionsFromVariable(AirflowPlugin):
    name = "variable_connection_plugin"
    hooks = []
    operators = [CreateConnectionsFromVariable]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
