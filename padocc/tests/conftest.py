
def pytest_collection_modifyitems(items):

    CLASS_ORDER = [
        "TestSetup",
        "TestFHs",
        "TestInit",
        "TestScan",
        "TestCompute",
        "TestValidate"
        #"TestCleanup"
    ]

    sorted_items = items.copy()
      # read the class names from default items
    class_mapping = {item: item.cls.__name__ for item in items}

    
    # Iteratively move tests of each class to the end of the test queue
    for class_ in CLASS_ORDER:
        sorted_items = [it for it in sorted_items if class_mapping[it] != class_] + [
            it for it in sorted_items if class_mapping[it] == class_
        ]
        
   
    items[:] = sorted_items