"""
Automatically scale pg_num based on how much data is stored in each pool.
"""

import json
import mgr_util
import threading
from typing import Any, Dict, List, Optional, Set, Tuple, TYPE_CHECKING, Union
import uuid
from prettytable import PrettyTable
from mgr_module import HealthChecksT, CLIReadCommand, CLIWriteCommand, CRUSHMap, MgrModule, Option, OSDMap

"""
Some terminology is made up for the purposes of this module:

 - "raw pgs": pg count after applying replication, i.e. the real resource
              consumption of a pool.
 - "grow/shrink" - increase/decrease the pg_num in a pool
 - "crush subtree" - non-overlapping domains in crush hierarchy: used as
                     units of resource management.
"""

INTERVAL = 5

PG_NUM_MIN = 32  # unless specified on a per-pool basis

if TYPE_CHECKING:
    import sys
    if sys.version_info >= (3, 8):
        from typing import Literal
    else:
        from typing_extensions import Literal

    ScaleModeT = Literal['scale-up', 'scale-down']


def nearest_power_of_two(n: int) -> int:
    v = int(n)

    v -= 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16

    # High bound power of two
    v += 1

    # Low bound power of tow
    x = v >> 1

    return x if (v - n) > (n - x) else v


def effective_target_ratio(target_ratio: float,
                           total_target_ratio: float,
                           total_target_bytes: int,
                           capacity: int) -> float:
    """
    Returns the target ratio after normalizing for ratios across pools and
    adjusting for capacity reserved by pools that have target_size_bytes set.
    """
    target_ratio = float(target_ratio)
    if total_target_ratio:
        target_ratio = target_ratio / total_target_ratio

    if total_target_bytes and capacity:
        fraction_available = 1.0 - min(1.0, float(total_target_bytes) / capacity)
        target_ratio *= fraction_available

    return target_ratio


class PgAdjustmentProgress(object):
    """
    Keeps the initial and target pg_num values
    """

    def __init__(self, pool_id: int, pg_num: int, pg_num_target: int) -> None:
        self.ev_id = str(uuid.uuid4())
        self.pool_id = pool_id
        self.reset(pg_num, pg_num_target)

    def reset(self, pg_num: int, pg_num_target: int) -> None:
        self.pg_num = pg_num
        self.pg_num_target = pg_num_target

    def update(self, module: MgrModule, progress: float) -> None:
        desc = 'increasing' if self.pg_num < self.pg_num_target else 'decreasing'
        module.remote('progress', 'update', self.ev_id,
                      ev_msg="PG autoscaler %s pool %d PGs from %d to %d" %
                      (desc, self.pool_id, self.pg_num, self.pg_num_target),
                      ev_progress=progress,
                      refs=[("pool", self.pool_id)])


class CrushSubtreeResourceStatus:
    def __init__(self) -> None:
        self.root_ids: List[int] = []
        self.osds: Set[int] = set()
        self.osd_count: Optional[int] = None  # Number of OSDs
        self.pg_target: Optional[int] = None  # Ideal full-capacity PG count?
        self.pg_current = 0  # How many PGs already?
        self.pg_left = 0
        self.capacity: Optional[int] = None  # Total capacity of OSDs in subtree
        self.pool_ids: List[int] = []
        self.pool_names: List[str] = []
        self.pool_count: Optional[int] = None
        self.pool_used = 0
        self.total_target_ratio = 0.0
        self.total_target_bytes = 0  # including replication / EC overhead


class PgAutoscaler(MgrModule):
    """
    PG autoscaler.
    """
    NATIVE_OPTIONS = [
        'mon_target_pg_per_osd',
        'mon_max_pg_per_osd',
    ]

    MODULE_OPTIONS = [
        Option(
            name='sleep_interval',
            type='secs',
            default=60),
        Option(
            'autoscale_profile',
            default='scale-up',
            type='str',
            desc='pg_autoscale profiler',
            long_desc=('Determines the behavior of the autoscaler algorithm '
                       '`scale-up` means that it starts out with minmum pgs '
                       'and scales up when there is pressure, `scale-down` '
                       'means starts out with full pgs and scales down when '
                       'there is pressure '),
            runtime=True),
    ]

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(PgAutoscaler, self).__init__(*args, **kwargs)
        self._shutdown = threading.Event()
        self._event: Dict[int, PgAdjustmentProgress] = {}

        # So much of what we do peeks at the osdmap that it's easiest
        # to just keep a copy of the pythonized version.
        self._osd_map = None
        if TYPE_CHECKING:
            self.autoscale_profile: 'ScaleModeT' = 'scale-up'
            self.sleep_interval = 60
            self.mon_target_pg_per_osd = 0

    def config_notify(self) -> None:
        for opt in self.NATIVE_OPTIONS:
            setattr(self,
                    opt,
                    self.get_ceph_option(opt))
            self.log.debug(' native option %s = %s', opt, getattr(self, opt))
        for opt in self.MODULE_OPTIONS:
            setattr(self,
                    opt['name'],
                    self.get_module_option(opt['name']))
            self.log.debug(' mgr option %s = %s',
                           opt['name'], getattr(self, opt['name']))
        # if the profiler option is not set, this means it is an old cluster
        autoscale_profile = self.get_module_option("autoscale_profile")
        if not autoscale_profile:
            self.set_module_option("autoscale_profile", "scale-up")

    @CLIReadCommand('osd pool autoscale-status')
    def _command_autoscale_status(self, format: str = 'plain') -> Tuple[int, str, str]:
        """
        report on pool pg_num sizing recommendation and intent
        """
        osdmap = self.get_osdmap()
        pools = osdmap.get_pools_by_name()
        profile = self.autoscale_profile
        ps, root_map = self._get_pool_status(osdmap, pools, profile)

        if format in ('json', 'json-pretty'):
            return 0, json.dumps(ps, indent=4, sort_keys=True), ''
        else:
            table = PrettyTable(['POOL', 'SIZE', 'TARGET SIZE',
                                 'RATE', 'RAW CAPACITY',
                                 'RATIO', 'TARGET RATIO',
                                 'EFFECTIVE RATIO',
                                 'BIAS',
                                 'PG_NUM',
#                                 'IDEAL',
                                 'NEW PG_NUM', 'AUTOSCALE'],
                                border=False)
            table.left_padding_width = 0
            table.right_padding_width = 2
            table.align['POOL'] = 'l'
            table.align['SIZE'] = 'r'
            table.align['TARGET SIZE'] = 'r'
            table.align['RATE'] = 'r'
            table.align['RAW CAPACITY'] = 'r'
            table.align['RATIO'] = 'r'
            table.align['TARGET RATIO'] = 'r'
            table.align['EFFECTIVE RATIO'] = 'r'
            table.align['BIAS'] = 'r'
            table.align['PG_NUM'] = 'r'
#            table.align['IDEAL'] = 'r'
            table.align['NEW PG_NUM'] = 'r'
            table.align['AUTOSCALE'] = 'l'
            for p in ps:
                if p['would_adjust']:
                    final = str(p['pg_num_final'])
                else:
                    final = ''
                if p['target_bytes'] > 0:
                    ts = mgr_util.format_bytes(p['target_bytes'], 6)
                else:
                    ts = ''
                if p['target_ratio'] > 0.0:
                    tr = '%.4f' % p['target_ratio']
                else:
                    tr = ''
                if p['effective_target_ratio'] > 0.0:
                    etr = '%.4f' % p['effective_target_ratio']
                else:
                    etr = ''
                table.add_row([
                    p['pool_name'],
                    mgr_util.format_bytes(p['logical_used'], 6),
                    ts,
                    p['raw_used_rate'],
                    mgr_util.format_bytes(p['subtree_capacity'], 6),
                    '%.4f' % p['capacity_ratio'],
                    tr,
                    etr,
                    p['bias'],
                    p['pg_num_target'],
#                    p['pg_num_ideal'],
                    final,
                    p['pg_autoscale_mode'],
                ])
            return 0, table.get_string(), ''

    @CLIWriteCommand("osd pool set autoscale-profile scale-up")
    def set_profile_scale_up(self) -> Tuple[int, str, str]:
        """
        set the autoscaler behavior to start out with minimum pgs and scales up when there is pressure
        """
        if self.autoscale_profile == "scale-up":
            return 0, "", "autoscale-profile is already a scale-up!"
        else:
            self.set_module_option("autoscale_profile", "scale-up")
            return 0, "", "autoscale-profile is now scale-up"

    @CLIWriteCommand("osd pool set autoscale-profile scale-down")
    def set_profile_scale_down(self) -> Tuple[int, str, str]:
        """
        set the autoscaler behavior to start out with full pgs and
        scales down when there is pressure
        """
        if self.autoscale_profile == "scale-down":
            return 0, "", "autoscale-profile is already a scale-down!"
        else:
            self.set_module_option("autoscale_profile", "scale-down")
            return 0, "", "autoscale-profile is now scale-down"

    def serve(self) -> None:
        self.config_notify()
        while not self._shutdown.is_set():
            self._maybe_adjust()
            self._update_progress_events()
            self._shutdown.wait(timeout=self.sleep_interval)

    def shutdown(self) -> None:
        self.log.info('Stopping pg_autoscaler')
        self._shutdown.set()

    def identify_subtrees_and_overlaps(self,
                                       osdmap: OSDMap,
                                       crush: CRUSHMap,
                                       result: Dict[int, CrushSubtreeResourceStatus],
                                       overlapped_roots: Set[int],
                                       roots: List[CrushSubtreeResourceStatus]) -> \
        Tuple[List[CrushSubtreeResourceStatus],
              Set[int]]:

        # We identify subtrees and overlapping roots from osdmap
        for pool_id, pool in osdmap.get_pools().items():
            crush_rule = crush.get_rule_by_id(pool['crush_rule'])
            assert crush_rule is not None
            cr_name = crush_rule['rule_name']
            root_id = crush.get_rule_root(cr_name)
            assert root_id is not None
            osds = set(crush.get_osds_under(root_id))

            # Are there overlapping roots?
            s = None
            for prev_root_id, prev in result.items():
                if osds & prev.osds:
                    s = prev
                    if prev_root_id != root_id:
                        overlapped_roots.add(prev_root_id)
                        overlapped_roots.add(root_id)
                        self.log.error('pool %d has overlapping roots: %s',
                                       pool_id, overlapped_roots)
                    break
            if not s:
                s = CrushSubtreeResourceStatus()
                roots.append(s)
            result[root_id] = s
            s.root_ids.append(root_id)
            s.osds |= osds
            s.pool_ids.append(pool_id)
            s.pool_names.append(pool['pool_name'])
            s.pg_current += pool['pg_num_target'] * pool['size']
            target_ratio = pool['options'].get('target_size_ratio', 0.0)
            if target_ratio:
                s.total_target_ratio += target_ratio
            else:
                target_bytes = pool['options'].get('target_size_bytes', 0)
                if target_bytes:
                    s.total_target_bytes += target_bytes * osdmap.pool_raw_used_rate(pool_id)
        return roots, overlapped_roots

    def get_subtree_resource_status(self,
                                    osdmap: OSDMap,
                                    crush: CRUSHMap) -> Tuple[Dict[int, CrushSubtreeResourceStatus],
                                                              Set[int]]:
        """
        For each CRUSH subtree of interest (i.e. the roots under which
        we have pools), calculate the current resource usages and targets,
        such as how many PGs there are, vs. how many PGs we would
        like there to be.
        """
        result: Dict[int, CrushSubtreeResourceStatus] = {}
        roots: List[CrushSubtreeResourceStatus] = []
        overlapped_roots: Set[int] = set()
        # identify subtrees and overlapping roots
        roots, overlapped_roots = self.identify_subtrees_and_overlaps(osdmap,
                                                                      crush, result, overlapped_roots, roots)
        # finish subtrees
        all_stats = self.get('osd_stats')
        for s in roots:
            assert s.osds is not None
            s.osd_count = len(s.osds)
            s.pg_target = s.osd_count * self.mon_target_pg_per_osd
            s.pg_left = s.pg_target
            s.pool_count = len(s.pool_ids)
            capacity = 0
            for osd_stats in all_stats['osd_stats']:
                if osd_stats['osd'] in s.osds:
                    # Intentionally do not apply the OSD's reweight to
                    # this, because we want to calculate PG counts based
                    # on the physical storage available, not how it is
                    # reweighted right now.
                    capacity += osd_stats['kb'] * 1024

            s.capacity = capacity
            self.log.debug('root_ids %s pools %s with %d osds, pg_target %d',
                           s.root_ids,
                           s.pool_ids,
                           s.osd_count,
                           s.pg_target)

        return result, overlapped_roots

    def _calc_final_pg_target(
            self,
            p: Dict[str, Any],
            pool_name: str,
            root_map: Dict[int, CrushSubtreeResourceStatus],
            root_id: int,
            capacity_ratio: float,
            even_pools: Dict[str, Dict[str, Any]],
            bias: float,
            is_used: bool,
            profile: 'ScaleModeT',
    ) -> Union[Tuple[float, int, int], Tuple[None, None, None]]:
        """
        `profile` determines behaviour of the autoscaler.
        `is_used` flag used to determine if this is the first
        pass where the caller tries to calculate/adjust pools that has
        used_ratio > even_ratio else this is the second pass,
        we calculate final_ratio by giving it 1 / pool_count
        of the root we are currently looking at.
        """
        if profile == "scale-up":
            final_ratio = capacity_ratio
            # So what proportion of pg allowance should we be using?
            pg_target = root_map[root_id].pg_target
            assert pg_target is not None
            pool_pg_target = (final_ratio * pg_target) / p['size'] * bias
            final_pg_target = max(p.get('options', {}).get('pg_num_min', PG_NUM_MIN),
                                  nearest_power_of_two(pool_pg_target))

        else:
            if is_used:
                pool_count = root_map[root_id].pool_count
                assert pool_count is not None
                even_ratio = 1 / pool_count
                used_ratio = capacity_ratio

                if used_ratio > even_ratio:
                    root_map[root_id].pool_used += 1
                else:
                    # keep track of even_pools to be used in second pass
                    # of the caller function
                    even_pools[pool_name] = p
                    return None, None, None

                final_ratio = max(used_ratio, even_ratio)
                pg_target = root_map[root_id].pg_target
                assert pg_target is not None
                used_pg = final_ratio * pg_target
                root_map[root_id].pg_left -= int(used_pg)
                pool_pg_target = used_pg / p['size'] * bias

            else:
                pool_count = root_map[root_id].pool_count
                assert pool_count is not None
                final_ratio = 1 / (pool_count - root_map[root_id].pool_used)
                pool_pg_target = (final_ratio * root_map[root_id].pg_left) / p['size'] * bias

            final_pg_target = max(p.get('options', {}).get('pg_num_min', PG_NUM_MIN),
                                  nearest_power_of_two(pool_pg_target))

            self.log.info("Pool '{0}' root_id {1} using {2} of space, bias {3}, "
                          "pg target {4} quantized to {5} (current {6})".format(
                              p['pool_name'],
                              root_id,
                              capacity_ratio,
                              bias,
                              pool_pg_target,
                              final_pg_target,
                              p['pg_num_target']
                          ))

        return final_ratio, pool_pg_target, final_pg_target

    def _calc_pool_targets(
            self,
            osdmap: OSDMap,
            pools: Dict[str, Dict[str, Any]],
            crush_map: CRUSHMap,
            root_map: Dict[int, CrushSubtreeResourceStatus],
            pool_stats: Dict[int, Dict[str, int]],
            ret: List[Dict[str, Any]],
            threshold: float,
            is_used: bool,
            profile: 'ScaleModeT',
            overlapped_roots: Set[int],
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        """
        Calculates final_pg_target of each pools and determine if it needs
        scaling, this depends on the profile of the autoscaler. For scale-down,
        we start out with a full complement of pgs and only descrease it when other
        pools needs more pgs due to increased usage. For scale-up, we start out with
        the minimal amount of pgs and only scale when there is increase in usage.
        """
        even_pools: Dict[str, Dict[str, Any]] = {}
        for pool_name, p in pools.items():
            pool_id = p['pool']
            if pool_id not in pool_stats:
                # race with pool deletion; skip
                continue

            # FIXME: we assume there is only one take per pool, but that
            # may not be true.
            crush_rule = crush_map.get_rule_by_id(p['crush_rule'])
            assert crush_rule is not None
            cr_name = crush_rule['rule_name']
            root_id = crush_map.get_rule_root(cr_name)
            assert root_id is not None
            if root_id in overlapped_roots and profile == "scale-down":
                # for scale-down profile skip pools
                # with overlapping roots
                self.log.warn("pool %d contains an overlapping root %d"
                              "... skipping scaling", pool_id, root_id)
                continue
            capacity = root_map[root_id].capacity
            assert capacity is not None
            if capacity == 0:
                self.log.debug('skipping empty subtree %s', cr_name)
                continue

            raw_used_rate = osdmap.pool_raw_used_rate(pool_id)

            pool_logical_used = pool_stats[pool_id]['stored']
            bias = p['options'].get('pg_autoscale_bias', 1.0)
            target_bytes = 0
            # ratio takes precedence if both are set
            if p['options'].get('target_size_ratio', 0.0) == 0.0:
                target_bytes = p['options'].get('target_size_bytes', 0)

            # What proportion of space are we using?
            actual_raw_used = pool_logical_used * raw_used_rate
            actual_capacity_ratio = float(actual_raw_used) / capacity

            pool_raw_used = max(pool_logical_used, target_bytes) * raw_used_rate
            capacity_ratio = float(pool_raw_used) / capacity

            self.log.info("effective_target_ratio {0} {1} {2} {3}".format(
                p['options'].get('target_size_ratio', 0.0),
                root_map[root_id].total_target_ratio,
                root_map[root_id].total_target_bytes,
                capacity))

            target_ratio = effective_target_ratio(p['options'].get('target_size_ratio', 0.0),
                                                  root_map[root_id].total_target_ratio,
                                                  root_map[root_id].total_target_bytes,
                                                  capacity)

            capacity_ratio = max(capacity_ratio, target_ratio)
            final_ratio, pool_pg_target, final_pg_target = self._calc_final_pg_target(
                p, pool_name, root_map, root_id, capacity_ratio, even_pools, bias, is_used, profile)

            if final_ratio is None:
                continue

            adjust = False
            if (final_pg_target > p['pg_num_target'] * threshold or
                    final_pg_target < p['pg_num_target'] / threshold) and \
                    final_ratio >= 0.0 and \
                    final_ratio <= 1.0:
                adjust = True

            assert pool_pg_target is not None
            ret.append({
                'pool_id': pool_id,
                'pool_name': p['pool_name'],
                'crush_root_id': root_id,
                'pg_autoscale_mode': p['pg_autoscale_mode'],
                'pg_num_target': p['pg_num_target'],
                'logical_used': pool_logical_used,
                'target_bytes': target_bytes,
                'raw_used_rate': raw_used_rate,
                'subtree_capacity': capacity,
                'actual_raw_used': actual_raw_used,
                'raw_used': pool_raw_used,
                'actual_capacity_ratio': actual_capacity_ratio,
                'capacity_ratio': capacity_ratio,
                'target_ratio': p['options'].get('target_size_ratio', 0.0),
                'effective_target_ratio': target_ratio,
                'pg_num_ideal': int(pool_pg_target),
                'pg_num_final': final_pg_target,
                'would_adjust': adjust,
                'bias': p.get('options', {}).get('pg_autoscale_bias', 1.0),
            })

        return ret, even_pools

    def _get_pool_status(
            self,
            osdmap: OSDMap,
            pools: Dict[str, Dict[str, Any]],
            profile: 'ScaleModeT',
            threshold: float = 3.0,
    ) -> Tuple[List[Dict[str, Any]],
               Dict[int, CrushSubtreeResourceStatus]]:
        assert threshold >= 2.0

        crush_map = osdmap.get_crush()
        root_map, overlapped_roots = self.get_subtree_resource_status(osdmap, crush_map)
        df = self.get('df')
        pool_stats = dict([(p['id'], p['stats']) for p in df['pools']])

        ret: List[Dict[str, Any]] = []

        # Iterate over all pools to determine how they should be sized.
        # First call of _calc_pool_targets() is to find/adjust pools that uses more capacaity than
        # the even_ratio of other pools and we adjust those first.
        # Second call make use of the even_pools we keep track of in the first call.
        # All we need to do is iterate over those and give them 1/pool_count of the
        # total pgs.

        ret, even_pools = self._calc_pool_targets(osdmap, pools, crush_map, root_map,
                                                  pool_stats, ret, threshold, True, profile, overlapped_roots)

        if profile == "scale-down":
            # We only have adjust even_pools when we use scale-down profile
            ret, _ = self._calc_pool_targets(osdmap, even_pools, crush_map, root_map,
                                             pool_stats, ret, threshold, False, profile, overlapped_roots)

        return (ret, root_map)

    def _update_progress_events(self) -> None:
        osdmap = self.get_osdmap()
        pools = osdmap.get_pools()
        for pool_id in list(self._event):
            ev = self._event[pool_id]
            pool_data = pools.get(pool_id)
            if pool_data is None or pool_data['pg_num'] == pool_data['pg_num_target'] or ev.pg_num == ev.pg_num_target:
                # pool is gone or we've reached our target
                self.remote('progress', 'complete', ev.ev_id)
                del self._event[pool_id]
                continue
            ev.update(self, (ev.pg_num - pool_data['pg_num']) / (ev.pg_num - ev.pg_num_target))

    def _maybe_adjust(self) -> None:
        self.log.info('_maybe_adjust')
        osdmap = self.get_osdmap()
        if osdmap.get_require_osd_release() < 'nautilus':
            return
        pools = osdmap.get_pools_by_name()
        profile = self.autoscale_profile
        ps, root_map = self._get_pool_status(osdmap, pools, profile)

        # Anyone in 'warn', set the health message for them and then
        # drop them from consideration.
        too_few = []
        too_many = []
        bytes_and_ratio = []
        health_checks: Dict[str, Dict[str, Union[int, str, List[str]]]] = {}

        total_bytes = dict([(r, 0) for r in iter(root_map)])
        total_target_bytes = dict([(r, 0.0) for r in iter(root_map)])
        target_bytes_pools: Dict[int, List[int]] = dict([(r, []) for r in iter(root_map)])

        for p in ps:
            pool_id = p['pool_id']
            pool_opts = pools[p['pool_name']]['options']
            if pool_opts.get('target_size_ratio', 0) > 0 and pool_opts.get('target_size_bytes', 0) > 0:
                bytes_and_ratio.append(
                    'Pool %s has target_size_bytes and target_size_ratio set' % p['pool_name'])
            total_bytes[p['crush_root_id']] += max(
                p['actual_raw_used'],
                p['target_bytes'] * p['raw_used_rate'])
            if p['target_bytes'] > 0:
                total_target_bytes[p['crush_root_id']] += p['target_bytes'] * p['raw_used_rate']
                target_bytes_pools[p['crush_root_id']].append(p['pool_name'])
            if not p['would_adjust']:
                continue
            if p['pg_autoscale_mode'] == 'warn':
                msg = 'Pool %s has %d placement groups, should have %d' % (
                    p['pool_name'],
                    p['pg_num_target'],
                    p['pg_num_final'])
                if p['pg_num_final'] > p['pg_num_target']:
                    too_few.append(msg)
                else:
                    too_many.append(msg)

            if p['pg_autoscale_mode'] == 'on':
                # Note that setting pg_num actually sets pg_num_target (see
                # OSDMonitor.cc)
                r = self.mon_command({
                    'prefix': 'osd pool set',
                    'pool': p['pool_name'],
                    'var': 'pg_num',
                    'val': str(p['pg_num_final'])
                })

                # create new event or update existing one to reflect
                # progress from current state to the new pg_num_target
                pool_data = pools[p['pool_name']]
                pg_num = pool_data['pg_num']
                new_target = p['pg_num_final']
                if pool_id in self._event:
                    self._event[pool_id].reset(pg_num, new_target)
                else:
                    self._event[pool_id] = PgAdjustmentProgress(pool_id, pg_num, new_target)
                self._event[pool_id].update(self, 0.0)

                if r[0] != 0:
                    # FIXME: this is a serious and unexpected thing,
                    # we should expose it as a cluster log error once
                    # the hook for doing that from ceph-mgr modules is
                    # in.
                    self.log.error("pg_num adjustment on {0} to {1} failed: {2}"
                                   .format(p['pool_name'],
                                           p['pg_num_final'], r))

        if too_few:
            summary = "{0} pools have too few placement groups".format(
                len(too_few))
            health_checks['POOL_TOO_FEW_PGS'] = {
                'severity': 'warning',
                'summary': summary,
                'count': len(too_few),
                'detail': too_few
            }
        if too_many:
            summary = "{0} pools have too many placement groups".format(
                len(too_many))
            health_checks['POOL_TOO_MANY_PGS'] = {
                'severity': 'warning',
                'summary': summary,
                'count': len(too_many),
                'detail': too_many
            }

        too_much_target_bytes = []
        for root_id, total in total_bytes.items():
            total_target = int(total_target_bytes[root_id])
            capacity = root_map[root_id].capacity
            assert capacity is not None
            if total_target > 0 and total > capacity and capacity:
                too_much_target_bytes.append(
                    'Pools %s overcommit available storage by %.03fx due to '
                    'target_size_bytes %s on pools %s' % (
                        root_map[root_id].pool_names,
                        total / capacity,
                        mgr_util.format_bytes(total_target, 5, colored=False),
                        target_bytes_pools[root_id]
                    )
                )
            elif total_target > capacity and capacity:
                too_much_target_bytes.append(
                    'Pools %s overcommit available storage by %.03fx due to '
                    'collective target_size_bytes of %s' % (
                        root_map[root_id].pool_names,
                        total / capacity,
                        mgr_util.format_bytes(total_target, 5, colored=False),
                    )
                )
        if too_much_target_bytes:
            health_checks['POOL_TARGET_SIZE_BYTES_OVERCOMMITTED'] = {
                'severity': 'warning',
                'summary': "%d subtrees have overcommitted pool target_size_bytes" % len(too_much_target_bytes),
                'count': len(too_much_target_bytes),
                'detail': too_much_target_bytes,
            }

        if bytes_and_ratio:
            health_checks['POOL_HAS_TARGET_SIZE_BYTES_AND_RATIO'] = {
                'severity': 'warning',
                'summary': "%d pools have both target_size_bytes and target_size_ratio set" % len(bytes_and_ratio),
                'count': len(bytes_and_ratio),
                'detail': bytes_and_ratio,
            }

        self.set_health_checks(health_checks)
