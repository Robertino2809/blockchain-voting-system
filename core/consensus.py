from core.block import Block
from core.blockchain import Blockchain


class ConsensusEngine:
    """
    Implementira longest chain rule — čvor uvijek prihvaća
    najduži valjani lanac u mreži kao istiniti lanac.
    """

    def __init__(self, blockchain: Blockchain):
        self.blockchain = blockchain

    def resolve(self, chains_from_peers: list[list[dict]]) -> bool:
        """
        Prima liste lanaca od svih peer čvorova.
        Ako pronađe duži valjani lanac — zamjenjuje trenutni.
        Vraća True ako je lanac zamijenjen.
        """
        best_chain: list[dict] | None = None
        best_length = len(self.blockchain.chain)

        for chain_data in chains_from_peers:
            candidate = [Block.from_dict(b) for b in chain_data]

            if len(candidate) > best_length and self.blockchain.is_valid(candidate):
                best_length = len(candidate)
                best_chain = chain_data

        if best_chain:
            self.blockchain.chain = [Block.from_dict(b) for b in best_chain]
            return True

        return False

    def select_best_chain(
        self, chains: list[list[dict]]
    ) -> list[dict] | None:
        """
        Od danih lanaca vraća najduži valjani — bez mijenjanja
        trenutnog stanja. Korisno za testiranje i logging.
        """
        best: list[dict] | None = None
        best_length = 0

        for chain_data in chains:
            candidate = [Block.from_dict(b) for b in chain_data]
            if len(candidate) > best_length and self.blockchain.is_valid(candidate):
                best_length = len(candidate)
                best = chain_data

        return best