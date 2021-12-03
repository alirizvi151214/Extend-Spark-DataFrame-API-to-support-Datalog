class DLProgram:

    def __init__(self, dlv_rule=[]):
        self.dlv_rule = dlv_rule

    def add_rule(self, input_rule):
        self.dlv_rule.append(input_rule)

    def __repr__(self):
        return f"DLProgram({self.dlv_rule})"


class DLRule:

    def __init__(self, head, body=[]):
        self.head = head
        self.body = body

    def add_body(self, input_atom):
        self.body.append(input_atom)

    def __repr__(self):
        return f"DLRule({self.head}, {self.body})"


class DLAtom:

    def __init__(self,  predicate_name, args=[]):
        self.predicate_name = predicate_name
        self.args_list = args

    def add_args(self, input_arg):
        self.args_list.append(input_arg)

    def get_vars(self):
        result = []
        for arg in self.args_list:
            if type(arg) == DLVariable:
                result.append(arg)
        return result

    def get_cons(self):
        result = []
        for arg in self.args_list:
            if type(arg) == DLConstants:
                result.append(arg)
        return result

    def __repr__(self):
        return f"DLAtom({self.predicate_name},{self.args_list})"


class DLVariable:
    def __init__(self, name):
        self.name = name

    def __hash__(self):
        return self.name.__hash__()

    def __eq__(self, other):
        return self.name == other.name

    def __repr__(self):
        return f"{self.name}"


class DLConstants:
    def __init__(self, strings):
        self.strings = strings

    def __repr__(self):
        return f"{self.strings}"
