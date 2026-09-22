import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "PulseAlert — real-time price alerts",
  description: "Set a price alert in 10 seconds. Powered by Confluent Cloud, Flink SQL and Schema Registry.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
