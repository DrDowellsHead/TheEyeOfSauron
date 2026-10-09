import unittest
from types import SimpleNamespace

from telethon.tl import types

from eye.tg_polls import fetch_poll_voters_yes_union


class FakeClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests = []

    async def __call__(self, request):
        self.requests.append(request)
        return self.responses.pop(0)


class PollVotersTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_pages_and_positive_options_are_combined(self):
        answers = [
            SimpleNamespace(text="✅ Буду", option=b"yes"),
            SimpleNamespace(text="Смогу к 19:00", option=b"later"),
            SimpleNamespace(text="Не смогу", option=b"no"),
        ]
        poll_msg = SimpleNamespace(
            id=777,
            media=SimpleNamespace(
                poll=SimpleNamespace(answers=answers, public_voters=True),
            ),
        )
        client = FakeClient(
            [
                SimpleNamespace(
                    votes=[SimpleNamespace(peer=types.PeerUser(101))],
                    users=[SimpleNamespace(id=102)],
                    next_offset="second-page",
                ),
                SimpleNamespace(
                    votes=[SimpleNamespace(peer=types.PeerUser(103))],
                    users=[],
                    next_offset=None,
                ),
                SimpleNamespace(
                    votes=[SimpleNamespace(peer=types.PeerUser(102))],
                    users=[SimpleNamespace(id=104)],
                    next_offset=None,
                ),
            ]
        )

        voter_ids, option_texts = await fetch_poll_voters_yes_union(
            client=client,
            chat_peer="chat",
            poll_msg=poll_msg,
            votes_page_size=100,
            smart_sort=False,
        )

        self.assertEqual(voter_ids, {101, 102, 103, 104})
        self.assertEqual(option_texts, ["✅ Буду", "Смогу к 19:00"])
        self.assertEqual(
            [request.offset for request in client.requests],
            [None, "second-page", None],
        )
        self.assertEqual(
            [request.option for request in client.requests],
            [b"yes", b"yes", b"later"],
        )


if __name__ == "__main__":
    unittest.main()
