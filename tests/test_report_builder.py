import unittest

from eye.instruments import ICON, INSTR_FORMS, INSTRUMENT_ORDER
from eye.report_builder import build_report
from eye.text_utils import plural_ru


class ReportBuilderTests(unittest.TestCase):
    def test_generic_saxophones_are_rendered_and_counted(self):
        musicians = {
            571345749: "саксофон",
            281693971: "саксофон",
            5371152825: "саксофон",
            805789697: "саксофон",
        }

        report = build_report(
            "Проверка",
            ["Смогу"],
            set(musicians),
            musicians,
        )

        self.assertIn("🎷 4 саксофона", report)
        self.assertIn("📊 Всего проголосовало: 4 человека", report)
        self.assertIn("✅ Учтено по инструментам: 4 человека", report)
        self.assertIn("⚠️ Не найдено в базе: 0 человек", report)
        self.assertIn("🎼 Нужно Пультов: 4", report)

    def test_saxophone_subtypes_are_reported_separately(self):
        musicians = {
            1: "саксофон",
            2: "сопрано саксофон",
            3: "альт-саксофон",
            4: "бас саксофон",
        }

        report = build_report("Проверка", ["Да"], set(musicians), musicians)

        self.assertIn("🎷 1 саксофон", report)
        self.assertIn("🎷 1 сопрано-саксофон", report)
        self.assertIn("🎷 1 альт-саксофон", report)
        self.assertIn("🎷 1 бас-саксофон", report)

    def test_all_found_musicians_are_included_in_total(self):
        musicians = {
            1: "вторые скрипки",
            2: "трубы",
            3: "тубы",
            4: "арфы",
            5: "редкий инструмент",
        }

        report = build_report("Проверка", ["Да"], set(musicians), musicians)

        self.assertIn("🎻 1 вторая скрипка", report)
        self.assertIn("🎺 1 труба", report)
        self.assertIn("🎺 1 туба", report)
        self.assertIn("🎶 1 арфа", report)
        self.assertIn("🎵 1 редкий инструмент", report)
        self.assertIn("📊 Всего проголосовало: 5 человек", report)

    def test_missing_voters_are_counted_and_listed_by_id(self):
        musicians = {
            10: "флейта",
            20: "",
            999: "труба",
        }

        report = build_report("Проверка", ["Да"], {10, 20, 30, 40}, musicians)

        self.assertIn("🎵 1 флейта", report)
        self.assertIn("❓ 1 неизвестный", report)
        self.assertIn("📊 Всего проголосовало: 4 человека", report)
        self.assertIn("✅ Учтено по инструментам: 2 человека", report)
        self.assertIn("⚠️ Не найдено в базе: 2 человека", report)
        self.assertIn("🔎 Telegram ID вне базы: 30, 40", report)
        self.assertNotIn("999", report)

    def test_every_standard_category_is_rendered(self):
        musicians = {
            index: instrument
            for index, instrument in enumerate(INSTRUMENT_ORDER, start=1)
        }

        report = build_report("Проверка", ["Да"], set(musicians), musicians)

        for instrument in INSTRUMENT_ORDER:
            with self.subTest(instrument=instrument):
                singular = INSTR_FORMS[instrument][0]
                self.assertIn(f"{ICON[instrument]} 1 {singular}", report)
        self.assertIn(
            f"📊 Всего проголосовало: {len(musicians)} человека",
            report,
        )

    def test_counts_and_desks_for_every_standard_category(self):
        paired = {"первые скрипки", "вторые скрипки", "альт", "виолончель"}

        for instrument in INSTRUMENT_ORDER:
            for amount in (1, 2, 5):
                with self.subTest(instrument=instrument, amount=amount):
                    musicians = {
                        user_id: instrument
                        for user_id in range(1, amount + 1)
                    }
                    report = build_report(
                        "Проверка",
                        ["Да"],
                        set(musicians),
                        musicians,
                    )

                    forms = INSTR_FORMS[instrument]
                    group_name = plural_ru(amount, *forms)
                    people_word = plural_ru(
                        amount,
                        "человек",
                        "человека",
                        "человек",
                    )
                    desks = (amount + 1) // 2 if instrument in paired else amount

                    self.assertIn(
                        f"{ICON[instrument]} {amount} {group_name}",
                        report,
                    )
                    self.assertIn(
                        f"📊 Всего проголосовало: {amount} {people_word}",
                        report,
                    )
                    self.assertIn(f"🎼 Нужно Пультов: {desks}", report)


if __name__ == "__main__":
    unittest.main()
