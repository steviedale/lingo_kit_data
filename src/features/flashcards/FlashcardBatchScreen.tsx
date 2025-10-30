import React, { useCallback } from "react";
import { useStudySessionLauncher } from "../shared/useStudySessionLauncher";

type FlashcardBatchScreenProps = {
  batchId: string;
  renderBatchOverview: (options: { startBatch: () => void; isLocked: boolean }) => React.ReactNode;
  renderFlashcardSession: (
    controls: { complete: () => void; exit: () => void },
    context: { batchId: string }
  ) => React.ReactNode;
  isLocked?: boolean;
};

const FlashcardBatchScreen: React.FC<FlashcardBatchScreenProps> = ({
  batchId,
  renderBatchOverview,
  renderFlashcardSession,
  isLocked = false,
}) => {
  const { launchSession, isSessionOpen } = useStudySessionLauncher();

  const handleStartBatch = useCallback(() => {
    launchSession({
      title: "Flashcards",
      render: (controls) => renderFlashcardSession(controls, { batchId }),
    });
  }, [batchId, launchSession, renderFlashcardSession]);

  return <>{renderBatchOverview({ startBatch: handleStartBatch, isLocked: isLocked || isSessionOpen })}</>;
};

export default FlashcardBatchScreen;
