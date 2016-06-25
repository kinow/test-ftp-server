
class MyDirWalker(object):

    def __init__(self):
        self.l = []
        self.count = 0

    def visit(self, line):
        actual_file = line.split(' ')[-1]
        self.l.append(actual_file)

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.l) == self.count:
            raise StopIteration
        elem = self.l[self.count]
        self.count += 1
        return elem

    def __str__(self):
        return self.l.__str__()