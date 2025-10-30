import React, { useCallback } from "react";
import { useStudySessionLauncher } from "../shared/useStudySessionLauncher";

type QuizBatchScreenProps = {
  batchId: string;
  renderBatchOverview: (options: { startBatch: () => void; isLocked: boolean }) => React.ReactNode;
  renderQuizSession: (
    controls: { complete: () => void; exit: () => void },
    context: { batchId: string }
  ) => React.ReactNode;
  isLocked?: boolean;
};

const QuizBatchScreen: React.FC<QuizBatchScreenProps> = ({
  batchId,
  renderBatchOverview,
  renderQuizSession,
  isLocked = false,
}) => {
  const { launchSession, isSessionOpen } = useStudySessionLauncher();

  const handleStartBatch = useCallback(() => {
    launchSession({
      title: "Quiz",
      render: (controls) => renderQuizSession(controls, { batchId }),
    });
  }, [batchId, launchSession, renderQuizSession]);

  return <>{renderBatchOverview({ startBatch: handleStartBatch, isLocked: isLocked || isSessionOpen })}</>;
};

export default QuizBatchScreen;
