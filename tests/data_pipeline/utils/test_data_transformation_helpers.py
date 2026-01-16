from data_pipeline.utils.data_transformation_helpers import format_list_natural_language

def test_format_list_natural_language_empty():
    assert format_list_natural_language([]) == ""
    assert format_list_natural_language(None) == ""

def test_format_list_natural_language_single():
    assert format_list_natural_language(["apple"]) == "apple"

def test_format_list_natural_language_two():
    assert format_list_natural_language(["apple", "banana"]) == "apple and banana"

def test_format_list_natural_language_multiple():
    assert format_list_natural_language(["apple", "banana", "cherry"]) == "apple, banana, and cherry"
    assert format_list_natural_language(["A", "B", "C", "D"]) == "A, B, C, and D"

def test_format_list_natural_language_deduplication():
    # Should maintain order of first appearance
    assert format_list_natural_language(["apple", "banana", "apple"]) == "apple and banana"

def test_format_list_natural_language_non_strings():
    assert format_list_natural_language([1, 2, 3]) == "1, 2, and 3"
