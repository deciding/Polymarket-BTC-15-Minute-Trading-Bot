"""Patch py_clob_client_v2 L2 headers for deposit-wallet auth.

For signature_type=3 (POLY_1271 / deposit wallet), the CLOB API expects the
L2 POLY_ADDRESS header to match the deposit wallet signer/funder address, not
the owner EOA address. The installed client always uses signer.address(), which
causes order submission to fail with:
  "the order signer address has to be the address of the API KEY"
"""

import logging


logger = logging.getLogger(__name__)

_patch_applied = False


def apply_clob_auth_patch() -> bool:
    global _patch_applied

    if _patch_applied:
        logger.info("CLOB auth patch already applied")
        return True

    try:
        from py_clob_client_v2.client import ClobClient, RequestArgs
        from py_clob_client_v2.headers.headers import create_level_2_headers

        def _patched_l2_headers(self, method: str, endpoint: str, body=None, serialized_body=None) -> dict:
            self.assert_level_2_auth()
            request_args = RequestArgs(
                method=method,
                request_path=endpoint,
                body=body,
                serialized_body=serialized_body,
            )

            headers = create_level_2_headers(
                self.signer,
                self.creds,
                request_args,
                timestamp=self._get_timestamp(),
            )

            # Deposit-wallet flow: L2 POLY_ADDRESS must match the deposit wallet.
            # Only override for order endpoints — balance/allowance endpoints
            # still need the EOA address for API-key auth.
            if int(self.builder.signature_type) == 3 and self.builder.funder:
                endpoint_lower = (endpoint or "").lower()
                if "order" in endpoint_lower:
                    headers["POLY_ADDRESS"] = self.builder.funder

            return headers

        ClobClient._l2_headers = _patched_l2_headers
        _patch_applied = True
        logger.info("CLOB auth patch applied for signature_type=3")
        return True
    except Exception as e:
        logger.error(f"Failed to apply CLOB auth patch: {e}")
        return False
