import os
import yaml

from padocc.core.filehandlers import (
    JSONFileHandler,
    KerchunkFile,
    TextFileHandler,
    LogFileHandler,
    CSVFileHandler

)

testdict = {
    0: 'test0',
    'test':None
}

testlist = ['test0', 'test','test2,3']

def generic(fh, testdata, dryrun):

    # Get/set
    fh.set(testdata)
    assert fh.get() == testdata

    # Create file/get filepath
    fh.create_file()
    assert dryrun == (not os.path.isfile(fh.filepath))

    # Magic methods
    assert 'test' in fh
    assert 'real' not in fh

    assert fh[0] == 'test0'
    fh[0] = 'test1'
    assert fh[0] == 'test1'

    if os.path.isfile(fh.filepath):
        os.system(f'rm -rf {fh.filepath}')
    
    assert not fh.file_exists()

    print(f' - Generic FH (dryrun={dryrun}) - Complete')

    return True

def generic_list(fh, testdata):

    # Magic methods
    assert str(fh) == '\n'.join(testdata)
    assert len(fh) == len(testdata)

    for x, item in enumerate(fh):
        assert item == testdata[x]

    # Append
    fh.append('testlist')
    assert fh[-1] == 'testlist'

    print(f' - Generic List FH - Complete')

    return True

class TestFHs:

    def test_json_fh(self):

        print("Unit Tests: JSON FH")

        for dryrun in [True, False]:
            json_fh = JSONFileHandler('auto_testdata_dir','testjs.json', dryrun=dryrun)

            json_fh.set(testdict)

            # Magic methods
            assert str(json_fh) == yaml.dump(testdict)

            # Generic
            assert generic(json_fh, testdict, dryrun)

            print(f' - JSON FH (dryrun={dryrun}) - Complete')

    def test_text_fh(self):

        print("Unit Tests: Text FH")

        for dryrun in [True, False]:

            text_fh = TextFileHandler('auto_testdata_dir', 'testtx.txt', dryrun=dryrun)

            text_fh.set(testlist)
            if dryrun:
                assert generic_list(text_fh, testlist)

            assert generic(text_fh, testlist, dryrun)

            print(f' - Text FH (dryrun={dryrun}) - Complete')

    def test_csv_fh(self):

        print("Unit Tests: CSV FH")

        for dryrun in [True, False]:

            csv_fh = CSVFileHandler('auto_testdata_dir', 'test.csv', dryrun=dryrun)

            csv_fh.set(testlist)

            for x, item in enumerate(csv_fh):
                assert item == testlist[x].split(',')

            assert generic(csv_fh, testlist, dryrun)

            csv_fh.update_status('testp','tests','jid1',dryrun)
            assert not len(csv_fh) == len(testlist)

            print(f' - CSV FH (dryrun={dryrun}) - Complete')

if __name__ == '__main__':
    fht = TestFHs()

    fht.test_json_fh()
    fht.test_text_fh()
    fht.test_csv_fh()





