import unittest

from eye.instruments import ICON, INSTR_FORMS, INSTRUMENT_ORDER, normalize_instrument


class InstrumentNormalizationTests(unittest.TestCase):
    def test_every_standard_category_is_preserved(self):
        for instrument in INSTRUMENT_ORDER:
            with self.subTest(instrument=instrument):
                self.assertEqual(normalize_instrument(instrument), instrument)

    def test_common_variants(self):
        variants = {
            "вторая скрипка": "вторые скрипки",
            "вторые скрипки": "вторые скрипки",
            "альт саксофон": "альт-саксофон",
            "трубы": "труба",
            "тубы": "туба",
            "Тубистка": "туба",
            "арфы": "арфа",
            "неизвестный": "неизвестно",
        }

        for raw, expected in variants.items():
            with self.subTest(raw=raw):
                self.assertEqual(normalize_instrument(raw), expected)

    def test_report_forms_normalize_back_to_their_category(self):
        for category, forms in INSTR_FORMS.items():
            for form in forms:
                with self.subTest(category=category, form=form):
                    self.assertEqual(normalize_instrument(form), category)

    def test_forms_icons_and_order_have_same_categories(self):
        self.assertEqual(set(INSTR_FORMS), set(ICON))
        self.assertEqual(set(INSTR_FORMS), set(INSTRUMENT_ORDER))


if __name__ == "__main__":
    unittest.main()
