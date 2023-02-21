import pytest

# @pytest.mark.parametrize(['test_input','expected'], [("3+5", 88), ("2+4", 6), ("6*9", 4)])
# def test_eval(test_input, expected):
# 	assert eval(test_input) == expected
	
	
	
# @pytest.fixture
# def foovalue():
# 	return {"this is": "foovalue"}

@pytest.fixture
def one():
	return 1
@pytest.fixture
def two():
	return 2
@pytest.fixture
def three():
	return 3
@pytest.fixture
def four():
	return 4





@pytest.mark.parametrize(
    ["a", "b"],
    [
		("one", 2),
		(1, "two"),
		(1, 2),
		("one", "two")
    ]
)
def test_multiply_is_even_request(a, b, request):
	"""Assert that an odd number times even is even."""
	if type(a) != int:
		a = request.getfixturevalue(a)
	if type(b) != int:
		b = request.getfixturevalue(b)

	assert a+b==3