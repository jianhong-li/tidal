package tidal.client;

public interface AdminCallback {

	public void onSuccess(final AdminResult adminResult);

    public void onException(final Throwable e);
}
