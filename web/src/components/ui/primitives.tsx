// shadcn-style UI primitives. Tiny, class-based, accessible enough for an
// admin panel. Each component forwards className via cn() so callers can tweak.
import { type ButtonHTMLAttributes, type InputHTMLAttributes, type ReactNode } from "react";
import { cn } from "../../lib/cn";

export function Button({
  className,
  variant,
  ...props
}: ButtonHTMLAttributes<HTMLButtonElement> & { variant?: "primary" | "danger" | "default" }) {
  return (
    <button
      className={cn(
        "btn",
        variant === "primary" && "btn-primary",
        variant === "danger" && "btn-danger",
        className,
      )}
      {...props}
    />
  );
}

export function Input({ className, ...props }: InputHTMLAttributes<HTMLInputElement>) {
  return <input className={cn("input", className)} {...props} />;
}

export function Card({ title, children, className }: { title?: ReactNode; children: ReactNode; className?: string }) {
  return (
    <div className={cn("card", className)}>
      {title != null && <div className="card-header">{title}</div>}
      {children}
    </div>
  );
}

export function Badge({ children, variant }: { children: ReactNode; variant?: "muted" | "danger" | "default" }) {
  return (
    <span className={cn("badge", variant === "muted" && "badge-muted", variant === "danger" && "badge-danger")}>
      {children}
    </span>
  );
}

export function Table({ head, children }: { head: ReactNode[]; children: ReactNode }) {
  return (
    <table className="table">
      <thead>
        <tr>
          {head.map((h, i) => (
            <th key={i}>{h}</th>
          ))}
        </tr>
      </thead>
      <tbody>{children}</tbody>
    </table>
  );
}
