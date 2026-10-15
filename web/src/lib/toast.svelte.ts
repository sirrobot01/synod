// A short confirmation after an action, such as "Renamed nas to backup-nas".
// Errors never use it: they stay anchored to the form that caused them.

class Toast {
  message = $state("");
  private timer: ReturnType<typeof setTimeout> | undefined;

  show(message: string) {
    this.message = message;
    clearTimeout(this.timer);
    this.timer = setTimeout(() => (this.message = ""), 5000);
  }

  hide() {
    clearTimeout(this.timer);
    this.message = "";
  }
}

export const toast = new Toast();
