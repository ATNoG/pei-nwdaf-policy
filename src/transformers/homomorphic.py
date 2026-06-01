"""
Homomorphic Encryption transformer using CKKS scheme (TenSEAL / Microsoft SEAL).

CKKS supports approximate arithmetic on real numbers, making it suitable for
floating-point network metrics and ML feature vectors.

Reference: Applying Homomorphic Encryption to Machine Learning Algorithms (DASH Harvard)
"""
import base64
import logging
import os
import socket
import struct
from pathlib import Path
from typing import Any

import tenseal as ts

from src.transformers.base import BaseTransformer

logger = logging.getLogger(__name__)

_POLY_MODULUS_DEGREE = 8192
# [60, 55, 60]: 175 bits total (within 218-bit limit for degree 8192).
# Scale 2^55 gives ~8 more bits of precision vs 2^40, enough for exact round-trip
# of normalized uint32 values (e.g. IPv4 addresses encoded as x/2^32-1 ∈ [0,1]).
_COEFF_MOD_BIT_SIZES = [60, 55, 60]
_GLOBAL_SCALE = 2**55


def _build_context() -> ts.Context:
    ctx = ts.context(
        ts.SCHEME_TYPE.CKKS,
        poly_modulus_degree=_POLY_MODULUS_DEGREE,
        coeff_mod_bit_sizes=_COEFF_MOD_BIT_SIZES,
    )
    ctx.global_scale = _GLOBAL_SCALE
    ctx.generate_relin_keys()
    return ctx


class HomomorphicEncryptionTransformer(BaseTransformer):
    """
    Encrypt selected fields with CKKS homomorphic encryption.

    Encrypted fields are replaced with FHE blobs; downstream services can
    perform homomorphic operations (addition, scalar multiply) without decrypting.
    Decryption requires HomomorphicDecryptionTransformer with the secret key.
    """

    def __init__(self, fields: list[str], key_dir: str = "./fhe_keys"):
        self.fields = set(fields)
        self.key_dir = key_dir
        self._context = self._load_or_create_context(key_dir)
        self.public_context_b64 = base64.b64encode(
            self._context.serialize()
        ).decode()

    def _load_or_create_context(self, key_dir: str) -> ts.Context:
        Path(key_dir).mkdir(parents=True, exist_ok=True)
        public_path = os.path.join(key_dir, "ckks_public.bin")
        secret_path = os.path.join(key_dir, "ckks_secret.bin")

        if os.path.exists(public_path) and os.path.exists(secret_path):
            logger.info("Loading existing CKKS public context from %s", public_path)
            with open(public_path, "rb") as f:
                return ts.context_from(f.read())

        logger.info("Generating new CKKS keys in %s", key_dir)
        ctx = _build_context()

        with open(secret_path, "wb") as f:
            f.write(ctx.serialize(save_secret_key=True))

        ctx.make_context_public()
        with open(public_path, "wb") as f:
            f.write(ctx.serialize())

        return ctx

    @staticmethod
    def _to_float(value: Any) -> tuple[float, str]:
        """Return (float_for_ckks, original_type_tag) so decryption can restore the original format."""
        if isinstance(value, bool):
            return float(value), "bool"
        if isinstance(value, int):
            return float(value), "int"
        if isinstance(value, float):
            return value, "float"
        if isinstance(value, str):
            try:
                return float(value), "str_float"
            except ValueError:
                pass
            try:
                packed = socket.inet_aton(value)
                # Normalize to [0, 1] — keeps CKKS error well below 0.5 for large uint32 values.
                return float(struct.unpack("!I", packed)[0]) / 4294967295.0, "ipv4"
            except (socket.error, OSError, struct.error):
                pass
            raise ValueError(
                f"Cannot convert string {value!r} to float for CKKS encryption. "
                "Expected a numeric string or an IPv4 address."
            )
        raise ValueError(
            f"Cannot convert {type(value).__name__} value to float for CKKS encryption."
        )

    async def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        result = data.copy()
        encrypted_fields = set(result.get("__fhe_encrypted_fields__") or [])

        for field in self.fields:
            if field not in result:
                continue
            val, original_type = self._to_float(result[field])
            enc = ts.ckks_vector(self._context, [val])
            result[field] = {
                "__fhe__": True,
                "ciphertext": base64.b64encode(enc.serialize()).decode(),
                "scheme": "CKKS",
                "original_type": original_type,
            }
            encrypted_fields.add(field)

        result["__fhe_context__"] = self.public_context_b64
        result["__fhe_encrypted_fields__"] = sorted(encrypted_fields)
        return result

    def __repr__(self) -> str:
        return f"HomomorphicEncryptionTransformer(fields={len(self.fields)}, key_dir={self.key_dir!r})"
