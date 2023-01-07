from zbta.btanalyzer.btanalyzer import BTAnalyzer
from zbta.attributes.common import ZBTACore
from zbta.attributes.core.core import CoreAccountMixin

class ZBTAGeneral (
    CoreAccountMixin,
    ZBTACore,
):

 def __init__(self, btanalyzer: BTAnalyzer) -> None:
    super().__init__(btanalyzer)
  