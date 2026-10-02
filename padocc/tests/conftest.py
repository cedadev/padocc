
def pytest_collection_modifyitems(items):

    CLASS_ORDER = [
        "TestSetup",
        "TestFHs",
        "TestInit",
        "TestScan",
        "TestCompute",
        "TestZarrCompute",
        "TestProject",
        "TestZarrValidate",
        "TestValidate",
        "TestGroup"
    ]
    
      # read the class names from default items
    class_mapping = {item: item.cls.__name__ for item in items}

    # Iteratively move tests of each class to the end of the test queue
    sorted_items = []
    for class_ in CLASS_ORDER:
        sorted_items += [it for it in items if class_mapping[it] == class_]

    items[:] = sorted_items