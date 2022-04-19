#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""sla miss execution_time on update current time fix

Revision ID: a982338e6d7f
Revises: 0e2a74e0fc9f
Create Date: 2020-09-01 13:48:03.093594

"""

# revision identifiers, used by Alembic.
revision = 'a982338e6d7f'
down_revision = '0e2a74e0fc9f'
branch_labels = None
depends_on = None

from alembic import op
from sqlalchemy.dialects import mysql
from sqlalchemy import text


def upgrade():
    conn = op.get_bind()
    if conn.dialect.name == "mysql":
        # NOTE:
        #
        # This patch is internal to Twitter, Here we explicitly set a default to the string literal
        # `CURRENT_TIMESTAMP(6)` to avoid the
        # default MySQL behavior for TIMESTAMP without `explicit_defaults_for_timestamp` turned
        # on as stated here:
        #
        #  "The first TIMESTAMP column in a table, if not explicitly declared with the NULL
        #  attribute or an explicit
        #   DEFAULT or ON UPDATE attribute, is automatically declared with the DEFAULT
        #   CURRENT_TIMESTAMP and
        #   ON UPDATE CURRENT_TIMESTAMP attributes." [0]
        #
        # Because of the "ON UPDATE CURRENT_TIMESTAMP" default, anytime the `sla_miss` table
        # is UPDATE'd without
        # explicitly re-passing the current value for the `execution_date` column, it will end up
        # getting clobbered with
        # the current timestamp value which breaks sla functionality and
        # causes duplicate alerts every minute.
        #
        # We unfortunately cannot turn `explicit_defaults_for_timestamp` on globally ourselves as
        # is now technically
        # required by Airflow [1], because this has to be set in the my.cnf and we don't control
        # that in managed MySQL.
        # A request to enable this fleet-wide has been made in MVP-18609.
        #
        # [0]: https://dev.mysql.com/doc/refman/5.6/en/server-system-variables.html
        # #sysvar_explicit_defaults_for_timestamp
        # [1]: https://github.com/apache/incubator-airflow/blob/master/UPDATING.md#mysql-setting
        # -required
        op.alter_column(table_name='sla_miss', column_name='execution_date',
            type_=mysql.TIMESTAMP(fsp=6), nullable=False,
            server_default=text('CURRENT_TIMESTAMP(6)'))


def downgrade():
    pass
