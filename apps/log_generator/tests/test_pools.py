"""실행 명령: PYTHONPATH=apps/log_generator/src python3 -m unittest discover -s apps/log_generator/tests"""

import random
import unittest
from collections import Counter

from log_generator import pools


class PoolTests(unittest.TestCase):
    def test_users_are_unique(self):
        # 크기 입력 검증은 config(PRODUCER_USER_POOL_SIZE)가 담당한다.
        users = pools.build_user_pool(500)
        self.assertEqual(len(users), 500)
        self.assertEqual(len(set(users)), 500)
        self.assertEqual((users[0], users[-1]), ("usr_00001", "usr_00500"))
        self.assertEqual(pools.build_user_pool(1), ("usr_00001",))
        self.assertEqual(len(pools.build_user_pool(10000)), 10000)

    def test_pool_contract_and_seeded_distribution(self):
        for values, weights in (
            (pools.PRODUCTS, pools.PRODUCT_WEIGHTS),
            (pools.LOCATIONS, pools.LOCATION_WEIGHTS),
            (pools.PAYMENT_METHODS, pools.PAYMENT_WEIGHTS),
            (pools.USER_TIERS, pools.USER_TIER_WEIGHTS),
            (pools.EVENT_KINDS, pools.EVENT_WEIGHTS),
        ):
            self.assertEqual(len(values), len(weights))
            self.assertEqual(len(set(values)), len(values))
            self.assertEqual(sum(weights), 100)
            self.assertTrue(all(weight > 0 for weight in weights))
            first = random.Random(42).choices(values, weights=weights, k=100000)
            second = random.Random(42).choices(values, weights=weights, k=100000)
            self.assertEqual(first, second)
            counts = Counter(first)
            for value, weight in zip(values, weights):
                self.assertAlmostEqual(counts[value] / len(first), weight / 100, delta=0.005)
        self.assertEqual(len(pools.PRODUCTS), 10)
        self.assertEqual(len({p.product_id for p in pools.PRODUCTS}), 10)
        for product in pools.PRODUCTS:
            self.assertIn(product.category, ("electronics", "fashion", "home"))
            self.assertGreater(product.price, 0)
            self.assertEqual(product.price % 1000, 0)
        # 지역 값은 pools.py가 기준이므로 목록 대신 공백 없는 값인지만 확인한다.
        for location in pools.LOCATIONS:
            self.assertTrue(all(v and v == v.strip() for v in location))
        with self.assertRaises(AttributeError):
            pools.PRODUCTS[0].price = 1
