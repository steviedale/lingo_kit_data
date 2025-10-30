import React from "react";
import { FullscreenSessionProvider } from "./FullscreenSessionProvider";

type WithFullscreenSessionProviderProps = {
  children: React.ReactNode;
};

const withFullscreenSessionProvider = <P extends object>(Component: React.ComponentType<P>) => {
  const Wrapped: React.FC<P> = (props) => (
    <FullscreenSessionProvider>
      <Component {...(props as P)} />
    </FullscreenSessionProvider>
  );

  Wrapped.displayName = `WithFullscreenSessionProvider(${Component.displayName ?? Component.name ?? "Component"})`;

  return Wrapped;
};

export const FullscreenSessionBoundary: React.FC<WithFullscreenSessionProviderProps> = ({ children }) => (
  <FullscreenSessionProvider>{children}</FullscreenSessionProvider>
);

export default withFullscreenSessionProvider;
