from padocc import Configuration

def test_init_basic():
    infile  = 'tests/data/myfile.csv'
    groupID = 'padocc-test-suite'

    kwargs = {}

    process = Configuration(label='test_init')

    process.init_config(infile, groupID=groupID, **kwargs)

if __name__ == '__main__':
    test_init_basic()