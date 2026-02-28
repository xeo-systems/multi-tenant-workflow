"use client";

import * as React from "react";
import { Input, InputProps } from "@/components/ui/input";

export interface PasswordInputProps extends Omit<InputProps, "type"> {}

export const PasswordInput = React.forwardRef<HTMLInputElement, PasswordInputProps>(
  (props, ref) => <Input ref={ref} type="password" {...props} />
);

PasswordInput.displayName = "PasswordInput";
