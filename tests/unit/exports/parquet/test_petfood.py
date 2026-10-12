import pytest

from openfoodfacts_exports.exports.parquet.petfood import PetFoodProduct

# Pet food analytical constituents, as sold per 100g
NUTRITION_PER_100G = {
    "aggregated_set": {
        "nutrients": {
            "crude-protein": {
                "source": "packaging",
                "source_index": 0,
                "source_per": "100g",
                "unit": "g",
                "value": 32.1,
            },
            "moisture": {
                "source": "packaging",
                "source_index": 0,
                "source_per": "100g",
                "unit": "g",
                "value": 8,
            },
        },
        "per": "100g",
        "preparation": "as_sold",
    },
}

# Most Open Pet Food Facts aggregated sets are expressed per 1kg, a basis that
# is not supported by the nutrition schema
NUTRITION_PER_1KG = {
    "aggregated_set": {
        "nutrients": {
            "crude-protein": {
                "source": "packaging",
                "source_index": 0,
                "source_per": "100g",
                "unit": "g",
                "value": 32.1,
            },
        },
        "per": "1kg",
        "preparation": "as_sold",
    },
}


class TestPetFoodProduct:
    @pytest.mark.parametrize(
        "data,expected",
        [
            (
                {"schema_version": 1004, "nutrition": NUTRITION_PER_100G},
                [
                    {
                        "name": "crude-protein",
                        "value": None,
                        "prepared_value": None,
                        "100g": 32.1,
                        "serving": None,
                        "unit": "g",
                        "prepared_100g": None,
                        "prepared_serving": None,
                        "prepared_unit": None,
                    },
                    {
                        "name": "moisture",
                        "value": None,
                        "prepared_value": None,
                        "100g": 8,
                        "serving": None,
                        "unit": "g",
                        "prepared_100g": None,
                        "prepared_serving": None,
                        "prepared_unit": None,
                    },
                ],
            ),
            # Unsupported `per` basis: no nutriments, rather than a failure
            ({"schema_version": 1004, "nutrition": NUTRITION_PER_1KG}, None),
            # Leftover (empty) legacy `nutriments` dict and no nutrition data
            ({"schema_version": 1004, "nutriments": {}}, None),
            # Leftover legacy `nutriments` dict next to the new `nutrition`
            # field: the latter wins
            (
                {
                    "schema_version": 1004,
                    "nutriments": {},
                    "nutrition": NUTRITION_PER_100G,
                },
                [
                    {
                        "name": "crude-protein",
                        "value": None,
                        "prepared_value": None,
                        "100g": 32.1,
                        "serving": None,
                        "unit": "g",
                        "prepared_100g": None,
                        "prepared_serving": None,
                        "prepared_unit": None,
                    },
                    {
                        "name": "moisture",
                        "value": None,
                        "prepared_value": None,
                        "100g": 8,
                        "serving": None,
                        "unit": "g",
                        "prepared_100g": None,
                        "prepared_serving": None,
                        "prepared_unit": None,
                    },
                ],
            ),
        ],
    )
    def test_parse_nutriments(self, data, expected):
        output = PetFoodProduct.parse_nutriments(data)
        assert output["nutriments"] == expected

    def test_product_with_unsupported_nutrition_basis_is_kept(self):
        product = PetFoodProduct(
            code="3017620422003",
            schema_version=1004,
            nutriments={},
            nutrition=NUTRITION_PER_1KG,
        )
        assert product.code == "3017620422003"
        assert product.nutriments is None

    def test_numeric_ingredient_quantity(self):
        product = PetFoodProduct(
            code="3017620422003",
            ingredients=[{"id": "en:chicken", "quantity": 1}],
        )
        assert product.ingredients is not None
        assert product.ingredients[0].quantity == "1"
