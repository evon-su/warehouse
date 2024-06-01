import copy
from collections import defaultdict
from functools import reduce, partial


pos_zero = 0.01
neg_zero = -0.01


class ArrangeWarehouse:
    def __init__(self, current_ware, house_cfg, exclude_tb):
        self.ware_limit = house_cfg['ware_limit']
        self.ware_near = house_cfg['ware_near']
        self.current_ware_orig = current_ware
        self.current_ware = copy.deepcopy(self.current_ware_orig)
        self.ware_struc = None     # Empty / Full / Half
        self.exclude_tb = exclude_tb
        self.ware_limit_space = reduce(lambda a, b: a + b, self.ware_limit.values())
        self.ware_len = len(self.current_ware)
        self.arrange_processes = []
        self.reset()

    def stocking(self, ware, cargo_name, cargo_group, space):
        if not self.current_ware[ware]['cargo_name']:
            self.current_ware[ware]['cargo_name'] = cargo_name
            self.current_ware[ware]['group_name'] = cargo_group
        self.current_ware[ware]['space'] += space

        if self.current_ware[ware]['space'] > self.ware_limit[ware] + 0.0001:
            raise ValueError("ArrangeWarehouse stocking ERROR...")

    def unloading(self, ware, space):
        self.current_ware[ware]['space'] += space
        if self.current_ware[ware]['space'] < pos_zero:
            self.current_ware[ware]['cargo_name'] = ''
            self.current_ware[ware]['group_name'] = ''
            self.current_ware[ware]['space'] = 0

    def stocking_process(self, cargo_name, cargo_group, space, plan, is_plan=True):
        ware = self.find_resid_ware(cargo_name)
        if ware > -1:
            residSpace = self.ware_limit[ware] - self.current_ware[ware]['space']
            if residSpace >= space:
                self.stocking(ware, cargo_name, cargo_group, space)
                if is_plan:
                    plan.append((ware, cargo_name, space))
                if residSpace - space < pos_zero:
                    self.update_ware_struc(ware, 2, 1)
                return 0
            else:
                self.stocking(ware, cargo_name, cargo_group, residSpace)
                if is_plan:
                    plan.append((ware, cargo_name, residSpace))
                self.update_ware_struc(ware, 2, 1)
            return space - residSpace
        else:
            return space

    def unloading_recursive(self, cargo_name, space, plan, is_plan=True):
        if not cargo_name:
            return True

        for ware_group in [2, 1]:
            for ware in self.ware_struc[ware_group]:
                if self.current_ware[ware]['cargo_name'] == cargo_name:
                    updateSpace = self.current_ware[ware]['space'] + space
                    if updateSpace > neg_zero:
                        if updateSpace > pos_zero:
                            self.unloading(ware, space)
                            if is_plan:
                                plan.append((ware, cargo_name, space))
                        else:
                            dis_space = -self.current_ware[ware]['space']
                            self.unloading(ware, dis_space)
                            if is_plan:
                                plan.append((ware, cargo_name, dis_space))
                            self.update_ware_struc(ware, ware_group, 0)
                            self.current_ware[ware]['pre_cargo'].append(self.current_ware[ware]['group_name'])
                            self.current_ware[ware]['pre_cargo'].pop(0)
                        return True
                    else:
                        dis_space = -self.current_ware[ware]['space']
                        self.unloading(ware, dis_space)
                        if is_plan:
                            plan.append((ware, cargo_name, dis_space))
                        self.update_ware_struc(ware, ware_group, 0)
                        self.current_ware[ware]['pre_cargo'].append(self.current_ware[ware]['group_name'])
                        self.current_ware[ware]['pre_cargo'].pop(0)
                        if self.unloading_recursive(cargo_name, updateSpace, plan):
                            return True
        return False

    def arrange_total(self, cargo_ls):
        if not self.check_cargo_ls_valid(cargo_ls):
            return False

        valid = self.traverse(cargo_ls)
        self.reset()
        return valid

    def traverse(self, cargo_ls):
        self.avail_ware_ls = self.gen_cargos_avail_ware_ls(cargo_ls)
        ns = [len(ls) for ls in self.avail_ware_ls if len(ls) > 0]
        repeat_ls = self.get_repeat_ls(cargo_ls)

        used = set()
        dic = defaultdict(set)
        self.count = 0
        return self.loop(self.avail_ware_ls, ns, partial(self.arrange, cargo_ls), used, dic, repeat_ls)

    def arrange(self, cargo_ls, ls):
        values = iter(ls)
        selec_ls = []
        for i in range(len(self.avail_ware_ls)):
            if self.avail_ware_ls[i]:
                selec_ls.append(self.avail_ware_ls[i][next(values)])
        self.reset()
        self.arrange_processes.clear()
        selec_gen = iter(selec_ls)
        empty_ls = self.ware_struc[0].copy()
        for v in selec_ls:
            if v in empty_ls:
                empty_ls.remove(v)
        valid = True
        space_dic = self.get_orig_cargo_space()
        current_space = reduce(lambda a, b: a + b, space_dic.values())

        for i in range(len(cargo_ls)):

            tmp_plan = []
            cargo_name, cargo_group, space = cargo_ls[i]
            if not cargo_name:
                self.arrange_processes.append([('', '', '')])
                continue
            updateSpace = space_dic[cargo_name] + space
            current_space += space
            if updateSpace < neg_zero or current_space > self.ware_limit_space:
                valid = False
                break
            space_dic[cargo_name] = updateSpace

            if space > 0:
                ware = next(selec_gen)
                if self.current_ware[ware]['cargo_name'] and \
                        (self.current_ware[ware]['cargo_name'] != cargo_name or
                         self.ware_limit[ware] - self.current_ware[ware]['space'] < neg_zero):
                    valid = False
                    break
                ware_resid = self.find_resid_ware(cargo_name)
                resid_space = self.stocking_process(cargo_name, cargo_group, space, tmp_plan)

                is_ware_no_repeat = -1 < ware_resid != ware
                if resid_space < pos_zero and (resid_space >= pos_zero and is_ware_no_repeat):
                    empty_ls.append(ware)

                if resid_space > pos_zero:
                    if self.ware_limit[ware] - self.current_ware[ware]['space'] > pos_zero:
                        if ware_resid == -1 or is_ware_no_repeat:
                            if self.is_previous_exclude(ware, cargo_group) or self.is_near_exclude(ware, cargo_group):
                                valid = False
                                break
                            resid_space = self.stocking_semi(ware, cargo_name, cargo_group, resid_space, tmp_plan)

                    if resid_space > pos_zero and not self.stocking_semi_recursive(cargo_name, cargo_group, resid_space,
                                                                                   empty_ls, tmp_plan):
                        valid = False
                        break
            else:
                if not self.unloading_recursive(cargo_name, space, tmp_plan):
                    valid = False
                    break
            self.arrange_processes.append(tmp_plan)

        if valid:
            return True

        self.count += 1
        if self.count > 10000:
            return False

    def loop(self, avail_ware_ls, container, func, used, dic, repeat_ls):
        def fn(i, idxs):
            return func([i] + idxs)

        if len(container) > 0:
            for i in range(container[0]):
                used2 = used.copy()
                dic2 = defaultdict(set)
                for k in dic:
                    dic2[k] = dic[k].copy()

                idx, valid, cargo, cargo_group = self.get_next(repeat_ls)
                if self.is_near_exclude(avail_ware_ls[idx][i], cargo_group):
                    continue
                if i not in used2 or (valid is False and i in dic2[cargo]):
                    used2.add(i)
                    dic2[cargo].add(i)
                    return self.loop(avail_ware_ls[idx + 1:], container[1:], partial(fn, i), used2, dic2, repeat_ls[idx + 1:])
        else:
            return func([])

    def current_ware_to_struc(self):
        ware_struc = [[], [], []]
        for ware in self.current_ware:
            cargo_name = self.current_ware[ware]['cargo_name']
            if not cargo_name:
                ware_struc[0].append(ware)
            elif self.ware_limit[ware] - self.current_ware[ware]['space'] < pos_zero:
                ware_struc[1].append(ware)
            else:
                ware_struc[2].append(ware)
        return ware_struc

    def is_near_exclude(self, ware, group_name):
        for ware_near in self.ware_near[ware]:
            if self.current_ware[ware_near]['cargo_name'] and \
               self.exclude_tb[group_name][self.current_ware[ware_near]['group_name']] == -1:
                return True
        return False

    def sort_by_near(self, cargo_group):
        first_ls = set()
        for ware in self.ware_struc[1] + self.ware_struc[2]:
            if self.current_ware[ware]['group_name'] == cargo_group:
                near_ls = self.ware_near[ware]
                for c in near_ls:
                    if c not in first_ls and c in self.ware_struc[0]:
                        first_ls.add(c)
                        yield c
        for ware in self.ware_struc[0]:
            if ware not in first_ls:
                yield ware

    def is_previous_exclude(self, ware, group_name):
        for pre_group in self.current_ware[ware]['pre_cargo']:
            if pre_group and self.exclude_tb[group_name][pre_group] == -1:
                return True
        return False

    def update_ware_struc(self, ware, from_, to_):
        self.ware_struc[from_].remove(ware)
        self.ware_struc[to_].append(ware)

    def find_resid_ware(self, cargo_name):
        for ware in self.ware_struc[2]:
            if self.current_ware[ware]['cargo_name'] == cargo_name:
                return ware
        return -1

    def get_current_ware_copy(self):
        return copy.deepcopy(self.current_ware)

    def check_cargo_ls_valid(self, cargo_ls):
        space_dic = self.get_orig_cargo_space()
        current_space = reduce(lambda a, b: a + b, space_dic.values())

        for item in cargo_ls:
            cargo_name = item[0]
            space = item[2]
            updateSpace = space_dic[cargo_name] + space
            current_space += space
            if updateSpace < neg_zero or current_space > self.ware_limit_space:
                return False
            space_dic[cargo_name] = updateSpace
        return True

    def stocking_semi(self, ware, cargo_name, cargo_group, space, plan, is_plan=True):
        ware_resid_space = self.ware_limit[ware] - self.current_ware[ware]['space']
        if space < ware_resid_space:
            self.stocking(ware, cargo_name, cargo_group, space)
            if is_plan:
                plan.append((ware, cargo_name, space))
            self.update_ware_struc(ware, 0, 2)
            return 0
        else:
            self.stocking(ware, cargo_name, cargo_group, ware_resid_space)
            if is_plan:
                plan.append((ware, cargo_name, ware_resid_space))
            self.update_ware_struc(ware, 0, 1)
            return space - ware_resid_space

    def stocking_semi_recursive(self, cargo_name, cargo_group, space, empty_ls, plan, is_plan=True):
        resid_space = space
        for ware in empty_ls.copy():
            if self.is_previous_exclude(ware, cargo_group) or self.is_near_exclude(ware, cargo_group):
                continue
            resid_space = self.stocking_semi(ware, cargo_name, cargo_group, resid_space, plan, is_plan)
            empty_ls.remove(ware)
            if resid_space < pos_zero:
                return True
        return False

    def get_orig_cargo_space(self):
        dic = defaultdict(int)
        for ware in self.current_ware_orig:
            if self.current_ware_orig[ware]['space'] > 0:
                dic[self.current_ware_orig[ware]['cargo_name']] += self.current_ware_orig[ware]['space']
        return dic

    def get_plan(self):
        return copy.deepcopy(self.arrange_processes)

    @staticmethod
    def copy_ware_struc(ware_struc):
        return [item.copy() for item in ware_struc]

    def gen_cargos_avail_ware_ls(self, cargo_ls):
        avail_ware_ls = []
        for cargo, cargo_group, space in cargo_ls:
            if space <= pos_zero or not cargo:
                avail_ware_ls.append([])
            else:
                avail_ware_ls.append(list(filter(lambda ware: not self.is_previous_exclude(ware, cargo_group),
                                                 self.ware_struc[0])))
        return avail_ware_ls

    def get_next(self, repeat_ls):
        idx = 0
        for valid, cargo, cargo_group, space in repeat_ls:
            if space > pos_zero:
                return idx, valid, cargo, cargo_group
            idx += 1

    def get_repeat_ls(self, cargo_ls):
        used = set()
        ls = []
        for item in cargo_ls:
            cargo = item[0]
            cargo_group = item[1]
            space = item[2]
            if cargo and cargo not in used:
                ls.append((True, cargo, cargo_group, space))
                used.add(cargo)
            else:
                ls.append((False, cargo, cargo_group, space))
        return ls

    def reset(self):
        for ware in self.current_ware_orig:
            self.current_ware[ware]['cargo_name'] = self.current_ware_orig[ware]['cargo_name']
            self.current_ware[ware]['group_name'] = self.current_ware_orig[ware]['group_name']
            self.current_ware[ware]['space'] = self.current_ware_orig[ware]['space']
            self.current_ware[ware]['pre_cargo'] = self.current_ware_orig[ware]['pre_cargo'].copy()
        self.ware_struc = self.current_ware_to_struc()

    def print_current_ware(self):
        space_tot = 0
        limit_tot = 0
        dic = defaultdict(int)
        for ware in self.current_ware:
            print("| ", ware, self.current_ware[ware])
            dic[self.current_ware[ware]['cargo_name']] += self.current_ware[ware]['space']
            space_tot += self.current_ware[ware]['space']
            limit_tot += self.ware_limit[ware]
        print(f"Cargo INFO: {dic}, Total Space={space_tot}, Warehouse Space Limit={limit_tot}")

