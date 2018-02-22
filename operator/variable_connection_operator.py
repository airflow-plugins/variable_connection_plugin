"""
CreateAirflowConnectionsFromAirflowVariable

This operator expects a global Airflow connection variable. This operator
expects the Variable to have the following fields.
    - is_enabled      - Whether or not the connection should be created. This is
                        useful when modifying the variable with a new connection
                        that is incomplete.
    - organization    - This is a name to add to the connection string as a
                        suffix to maintain uniqueness when adding multiple
                        connections of the same type.
    - type            - The type of connection being created.
    - token           - The relevant token for the connection type.
    - instance_url    - The instance url for the service, if applciable.

If using encrypted tokens, make sure to set "is_encypted" to True and to
specify the id where the Fernet Key is kept. This operator will expect the
Fernet Key to be kept in the password section of the relevant connection.

Currently supported connection types include:
    - GOOGLE_ANALYTICS
    https://github.com/airflow-plugins/google_analytics_plugin/blob/master/hooks/google_analytics_hook.py
    - HUBSPOT
    https://github.com/airflow-plugins/hubspot_plugin/blob/master/hooks/hubspot_hook.py
    - SALESFORCE
    https://github.com/apache/incubator-airflow/blob/master/airflow/contrib/hooks/salesforce_hook.py

"""
from cryptography.fernet import Fernet

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator, Connection, Variable
from airflow.utils.db import provide_session


class CreateConnectionsFromVariable(BaseOperator):
    """
    Pulls from a global airflow variable, decrypts access_token
    and creates a new connection using that access_token.

    :param config_variable_key:     Name of the Variable containing the
                                    following parameters:
                                        - is_enabled
                                        - organization
                                        - type
                                        - token
                                        - instance_url
    :type config_variable_key:      string
    :param fernet_key_conn_id:      *(optional)* Name of conn id containing
                                    the relevant Fernet Key
                                    Default: None
    :type fernet_key_conn_id:       string
    :param is_encrypted:            *(optional)* Whether or not the Variable
                                    contains encrypted fields. Used in concert
                                    with fernet_key_conn_id.
                                    Default: False
    :type is_encrypted:             boolean
    """

    def __init__(self,
                 config_variable_key,
                 fernet_key_conn_id=None,
                 is_encrypted=True,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.fernet_key_conn_id = fernet_key_conn_id
        self.config_variable_key = config_variable_key

    def decrypt_str(self, fernet_key, encrypted_str):
        f = Fernet(fernet_key)

        encrypted_str = str.encode(encrypted_str)
        msg = f.decrypt(encrypted_str)
        msg = bytes.decode(msg)

        return msg

    @provide_session
    def merge_connection(self, connection, session=None):
        session.query(Connection).filter(Connection.conn_id == connection.conn_id).delete()
        session.add(connection)
        session.commit()

    def execute(self, context):
        # decryption setup
        secret_key_conn = BaseHook.get_connection(self.fernet_key_conn_id)

        # Fetch Latest Config
        dag_config_var = Variable.get(self.config_variable_key,
                                      default_var={},
                                      deserialize_json=True)

        for config_key in dag_config_var:
            org_config = dag_config_var[config_key]

            _is_enabled = org_config.get('is_enabled', False)
            _organization_name = org_config.get('organization', None)
            _type = org_config.get('type', None)
            _token = org_config.get('token', None)
            _instance_url = org_config.get('instance_url', None)

            # A disabled DAG could be improperly configured.
            # No need to push bad configs to airflow connections
            if _is_enabled is False:
                continue

            # Doing some checks before attempting to create and decrypt
            _enabled_types = ('GOOGLE_ANALYTICS',
                              'HUBSPOT',
                              'SALESFORCE')

            if _type not in _enabled_types:
                continue

            if not isinstance(_token, str):
                continue

            if not isinstance(_instance_url, str):
                continue

            # Decrypt Token
            access_token = self.decrypt_str(secret_key_conn.password, _token)

            # Set the right connection prefix
            if _type == 'SALESFORCE':
                conn_prefix = 'sf'
            elif _type == 'HUBSPOT':
                conn_prefix = 'hs'
                _instance_url = 'https://api.hubapi.com/'
            elif _type == 'GOOGLE_ANALYTICS':
                conn_prefix = 'ga'
            else:
                conn_prefix = ''

            # Create New Connections if Not Exists
            organization_connection = Connection(
                conn_id='{0}_{1}'.format(conn_prefix, _organization_name),
                conn_type='http',
                host=_instance_url,
                password=access_token,
                extra='{"auth_type": "direct"}'
            )

            self.merge_connection(organization_connection)
