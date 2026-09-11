# Copyright (c) 2026 Rafał Kozłowski <rafalkozlowski07@gmail.com>
# GNU General Public License v3.0+ (see COPYING or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

DOCUMENTATION = r'''
---
module: valkey_wait

version_added: "0.4.0"

author:
  - Rafał Kozłowski (@rkozlo)

short_description: Wait until Valkey will be in certain condition.

extends_documentation_fragment:
  - rkozlo.valkey.valkey_client_common

description:
  - This module waits until valkey be in desired state.
  - Can be used as orchestrator for example waiting until RDB is loading
    AOF is rewriting, replica is still syncing, valkey is not ready to accept connections.

attributes:
  check_mode:
    description: Supports check_mode.
    support: full
  idempotent:
    support: full
    description:
      - Module always returns changed=False.

options:
  state:
    description:
      - Desired state of Valkey.
      - In this moment it accepts only I(ready).
      - Will check if valkey response is reachable, passed authentication working and responding to ping.
    type: str
    default: ready
    choices: ['ready']
  interval:
    description:
      - Interval between checks in second.
    type: int
    default: 1
  retries:
    description:
      - How many tries until decides to fail.
      - Duration related with O(interval).
    type: int
    default: 60
  conditions:
    description:
      - Conditions module will observe if are met.
      - >
        You can use any field described in INFO ex. {role: slave}.
    type: dict
    required: false
'''

EXAMPLES = r'''
- name: Wait for server to start
  rkozlo.valkey.valkey_wait:

- name: Wait for server to start changing intervals
  rkozlo.valkey.valkey_wait:
    interval: 5
    retries: 10

- name: Wait for replica to connect
  rkozlo.valkey.valkey_wait:
    conditions:
      role: master
      connected_slaves: 1
      replicas_waiting_psync: 0

- name: Wait for replication to sync
  rkozlo.valkey.valkey_wait:
    conditions:
      role: slave
      master_link_status: up

- name: Wait rdb loading
  rkozlo.valkey.valkey_wait:
    conditions:
      loading: 0

- name: Wait aof finished to rewrite
  rkozlo.valkey.valkey_wait:
    conditions:
      aof_rewrite_in_progress: 0
'''

RETURN = r'''
---
changed:
  description: Whether the module made changes.
  returned: always
  type: bool
info:
  description: Statistics about wait status.
  returned: always
  type: dict
  sample: [{
        "state": {
            "current_retry": 1,
            "expected": "ready",
            "fail_retries": 0,
            "is_met": true,
            "last_check_at": "2026-09-06T08:45:44.136185",
            "last_fail_at": "2026-09-06T08:45:44.136185",
            "last_value": "ready"
        }
    }]
fail_waits:
  description: Statistics about failed waits.
  returned: on failure
  type: dict
  sample: [{
        "state": {
            "current_retry": 5,
            "expected": "ready",
            "fail_retries": 0,
            "is_met": false,
            "last_check_at": "2026-09-06T08:45:44.136185",
            "last_fail_at": "2026-09-06T08:45:44.136185",
            "last_value": "down"
        }
    }]
'''


from time import sleep
from datetime import datetime
from ansible.module_utils.basic import AnsibleModule
from ansible.module_utils.common.text.converters import to_native
from ansible_collections.rkozlo.valkey.plugins.module_utils.valkey import get_client_common_argument_spec, get_main_conn_kwargs
from ansible_collections.rkozlo.valkey.plugins.module_utils.valkey_client import ValkeyClient

try:
    import valkey.exceptions
    HAS_VALKEY_PACKAGE = True
except ImportError:
    HAS_VALKEY_PACKAGE = False
    Valkey = None
    valkey_exceptions = None


class ValkeyWait:
    def __init__(self, module, client):
        self.module = module
        self.client = client
        self.interval = module.params['interval']
        self.retries = module.params['retries']
        self.state = module.params['state']
        self.conditions = module.params['conditions']
        self._retry = 0
        self.statistics = self._initialize_statistics()

    def _initialize_statistics(self):
        statistics = {}
        base_info = {
            'current_retry': 0,
            'fail_retries': 0,
            'last_check_at': None,
            'last_fail_at': None,
            'is_met': False
        }
        if self.state:
            statistics['state'] = {
                **base_info,
                'last_value': 'down',
                'expected': self.state,
            }
        if self.conditions:
            for condition, val in self.conditions.items():
                statistics[condition] = {
                    **base_info,
                    'last_value': None,
                    'expected': val,
                }
        return statistics

    def _fetch_info(self):
        return self.client._execute('info')

    def _wait_for_state(self):
        try:
            self.client.client.ping()
        except valkey.exceptions.ConnectionError:
            return 'down'
        except valkey.exceptions.ResponseError:
            return 'rejected'
        except Exception as e:
            self.module.fail_json(msg="Unexpected exception: %s" % to_native(e))
        return 'ready'

    def run(self):
        '''Run conditions and gather statistics.
        Success when all of the conditions in single run met.
        '''
        while self._retry < self.retries:
            self._retry += 1

            return_state = self._wait_for_state()
            self._set_statistics('state', return_state)
            # State not ready so server will not be able to execute INFO command
            if self.conditions and self.statistics['state']['is_met']:
                info_result = self._fetch_info()
                for cond in self.conditions.keys():
                    try:
                        value = info_result[cond]
                    except KeyError:
                        self.module.fail_json(msg=f'Passed condition {cond} was not found in INFO result')
                    self._set_statistics(cond, value)
            if not self.return_failed():
                return True
            sleep(self.interval)
        return False

    def _set_statistics(self, name, returned):
        ts = datetime.now()
        is_met = str(returned) == str(self.statistics[name]['expected'])
        self.statistics[name]['is_met'] = is_met
        self.statistics[name]['last_value'] = returned
        self.statistics[name]['last_check_at'] = ts
        self.statistics[name]['current_retry'] += 1
        if not is_met:
            self.statistics[name]['fail_retries'] += 1
            self.statistics[name]['last_fail_at'] = ts

    def return_summary(self):
        return self.statistics

    def return_failed(self):
        return {k: v for k, v in self.statistics.items() if not v.get('is_met')}


def main():
    argument_spec = get_client_common_argument_spec()
    argument_spec.update(
        interval=dict(type='int', default=1),
        retries=dict(type='int', default=60),
        state=dict(type='str', default='ready', choices=['ready']),
        conditions=dict(type='dict', required=False)
    )
    module = AnsibleModule(
        argument_spec=argument_spec,
        supports_check_mode=True,
    )

    conn_kwargs = get_main_conn_kwargs(module)
    client_kwargs = module.params.get('client_kwargs', {})
    conn_kwargs.update(client_kwargs)
    cluster = module.params['cluster']

    client = ValkeyClient(module, cluster, **conn_kwargs)

    valkey_wait = ValkeyWait(module, client)
    result = valkey_wait.run()
    if result:
        module.exit_json(changed=False, info=valkey_wait.return_summary())
    else:
        module.fail_json(
            changed=False,
            msg="Failed: " + ', '.join(valkey_wait.return_failed().keys()),
            info=valkey_wait.return_summary(),
            fail_waits=valkey_wait.return_failed()
        )


if __name__ == '__main__':
    main()
